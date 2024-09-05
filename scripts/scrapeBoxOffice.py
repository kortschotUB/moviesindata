import modin.pandas as md
import sys
sys.path.append('../library')
from core import createSlidingWindows, exceptionOutput
import json
from tqdm.asyncio import tqdm
import httpx
import asyncio
from aiolimiter import AsyncLimiter
import logging
import redis
from bs4 import BeautifulSoup
from uuid import uuid1
import user_agent
import re

# Configure logging --> This is configured to optimize for tqdm progress. Comment out if there are still unknown errors
logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress httpx informational printout
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)

# Suppress httpcore informational printout
httpcore_logger = logging.getLogger("httpcore")
httpcore_logger.setLevel(logging.WARNING)

# Establish connection to local redis server(s)
r1 = redis.Redis(
    host='127.0.0.1',
    port=6379,
    charset="utf-8",
    decode_responses=True,
    db=1
)
r2 = redis.Redis(
    host='127.0.0.1',
    port=6379,
    charset="utf-8",
    decode_responses=True,
    db=2
)

async def requestData(client, imdbId, bomId, limiter):
    async with limiter:
        try:
            url = f'https://www.boxofficemojo.com/release/{bomId}/?ref_=bo_di_table_29'
            headers = {'User-Agent': user_agent.generate_user_agent()}
            response = await client.get(url, headers=headers)
            weekends = False

            if response.status_code == 302:
                url = f'https://www.boxofficemojo.com/release/{bomId}/weekend/'
                response = await client.get(url, headers=headers)
                weekends = True

            # # Bail on this movie if we can't find it still
            # if response != 200:
            #     return
            
            soup = BeautifulSoup(response.content, 'html.parser')

            tables = soup.find_all('table')
            months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            days   = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

            for table in tables:
                rows = {
                    'isWeekend': weekends,
                    'bomId': bomId,
                    'imdbId': imdbId
                    }

                # Filter bad tables
                tableText = str(table)

                # Not date table
                if not (any(m for m in months if m in tableText) | any(d for d in days if d in tableText)):
                    continue

                tableData = []

                for row in table.find_all('tr'):
                    rowData = []
                    cols = row.find_all('td')
                    for col in cols:
                        data = col.text.strip()
                        rowData.append(data)
                    tableData.append(rowData)

                rows['tableData'] = tableData

                # Create primary key for live redis caching
                rowId = str(uuid1())            
                r2.set(rowId, json.dumps(rows))
                return

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            return

async def processWindow(ids: list):
    rateLimit = AsyncLimiter(100,1) # 100 / second

    async with httpx.AsyncClient() as client:
        tasks = [requestData(client, idMap[0], idMap[1], rateLimit) for idMap in ids]
        for task in asyncio.as_completed(tasks):
        # for task in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            await task
    
# Return keys
imdbIds = r1.keys()
bomIds = [e.split('/')[0] for e in r1.mget(imdbIds)]
idMap = [e for e in list(zip(imdbIds, bomIds)) if e[1] != 'unknown'] # Filter out items for which there is no box office information
print(len(idMap))
# Create batches of 5000
idsWindowed = createSlidingWindows(l = idMap, windowSize = 500, overlap = 0)

print(f"LOOKING FOR: {len(bomIds)} NEW IDs")
print(f"THEY LOOK LIKE THIS: {idMap[0]}")
print(f"WE'VE GOT {len(idsWindowed)} WINDOWS WITH AN AVG LENGTH OF {round(len(bomIds)/len(idsWindowed), 2)}")

async def main():
    for i, idWindow in enumerate(idsWindowed, start=1):
        print(f"RUNNING WINDOW: {i}/{len(idsWindowed)}")
        await processWindow(idWindow)

if __name__ == '__main__':
    asyncio.run(main())
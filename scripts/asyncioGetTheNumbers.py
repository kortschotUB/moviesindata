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
import user_agent
import re
import arrow
import os
from dotenv import load_dotenv
load_dotenv()

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
r = redis.Redis(
    host='127.0.0.1',
    port=6379,
    charset="utf-8",
    decode_responses=True,
    db=3
)

async def requestData(client, date, limiter):
    async with limiter:
        try:
            url = f'{os.getenv("BOX_OFFICE_URL")}{date}'
            headers = {'User-Agent': user_agent.generate_user_agent()}
            response = await client.get(url, headers=headers)

            if response.status_code == 403:
                await asyncio.sleep(2)            
            
            soup = BeautifulSoup(response.content, 'html.parser')

            # tables = soup.find_all('table', id='box_office_weekly_table')
            table = soup.find('table', id='box_office_weekly_table')

            tableData = []

            for row in table.find_all('tr'):
                rowData = []
                cols = row.find_all('td')
                for col in cols:
                    data = col.text.strip()
                    rowData.append(data)
                tableData.append(rowData)
            
            # Create primary key for live redis caching
            r.set(date, json.dumps(tableData))
            return

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            return

async def processWindow(dates: list):
    rateLimit = AsyncLimiter(5,1) # 100 / second

    async with httpx.AsyncClient() as client:
        tasks = [requestData(client, date, rateLimit) for date in dates]
        for task in asyncio.as_completed(tasks):
        # for task in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            await task
    
# Return keys
maxDate = arrow.get("2024/08/30")
minDate = arrow.get("1980/01/04")

totalDays = ((maxDate - minDate).total_seconds())/(60*60*24)

totalWeeks = int(totalDays / 7)

dates = []
for i in range(totalWeeks):
    date = maxDate.shift(days=-(i*7))
    dateFormatted = date.format("YYYY/MM/DD")
    dates.append(dateFormatted)

foundDates = r.keys("*")
datesFiltered = [d for d in dates if d not in foundDates]
print(f"WE ALREADY FOUND {len(foundDates)} UNIQUE WEEKS")
print(f"WE ARE NOW LOOKING FOR {len(datesFiltered)} UNIQUE WEEKS")

idsWindowed = createSlidingWindows(l = datesFiltered, windowSize = 25, overlap = 0)

async def main():
    for i, idWindow in enumerate(idsWindowed, start=1):
        print(f"RUNNING WINDOW: {i}/{len(idsWindowed)}")
        await processWindow(idWindow)

if __name__ == '__main__':
    asyncio.run(main())
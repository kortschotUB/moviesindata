import sys
sys.path.append('../library')
from core import createSlidingWindows, exceptionOutput
import time
import json
import httpx
import asyncio
from aiolimiter import AsyncLimiter
import logging
import redis
from bs4 import BeautifulSoup
import user_agent
from dotenv import load_dotenv
import ast
load_dotenv()

# Configure logging --> This is configured to optimize for tqdm progress. Comment out if there are still unknown errors
# logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# Suppress httpx informational printout
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)

# Suppress httpcore informational printout
httpcore_logger = logging.getLogger("httpcore")
httpcore_logger.setLevel(logging.WARNING)

# Establish connection to local redis server(s)
r5 = redis.Redis(
    host='127.0.0.1',
    port=6379,
    charset="utf-8",
    decode_responses=True,
    db=5
)

async def requestData(client, imdbId, limiter):
    async with limiter:
        try:
            headers = {'User-Agent': user_agent.generate_user_agent()}

            url = f'https://www.imdb.com/title/{imdbId}/'

            response = await client.get(url, headers=headers)

            if response.status_code != 200:
                # time.sleep(.5)
                # requestData(client, imdbId, limiter)
                return

            soup = BeautifulSoup(response.text, 'html.parser')

            rating = ast.literal_eval(str(soup).replace('null',"'None'").split('ratingsSummary":')[1].split('}')[0] + '}')['aggregateRating']
                
            # Create primary key for live redis caching
            r5.set(imdbId, rating)
            return

        except Exception as e:
            # logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            print(exceptionOutput(e))
            return

async def processWindow(links: list):
    rateLimit = AsyncLimiter(20,1) # 100 / second
    
    async with httpx.AsyncClient() as client:
        tasks = [requestData(client, link, rateLimit) for link in links]
        
        for task in asyncio.as_completed(tasks):
            await task


async def main():
    with open('../data/allIds.json') as f:
        ids = json.load(f)
    
    knownIds = r5.keys('*')

    idsFiltered = [i for i in ids if i not in knownIds]

    idsWindowed = createSlidingWindows(l = idsFiltered, windowSize = 20, overlap = 0)

    print(f"WE'RE LOOKING FOR {len(idsFiltered)} IDS")
    print(f"THIS MEANS WE HAVE {len(idsWindowed)} WINDOWS")

    for i, idWindow in enumerate(idsWindowed, start=1):
        print(f"RUNNING WINDOW: {i}/{len(idsWindowed)}")
        await processWindow(idWindow)

if __name__ == '__main__':
    asyncio.run(main())
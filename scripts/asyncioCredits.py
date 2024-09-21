import sys
sys.path.append('../library')
from core import createSlidingWindows
from tqdm.asyncio import tqdm
import httpx
import asyncio
from aiolimiter import AsyncLimiter
import json
import logging
import redis
import os
from dotenv import load_dotenv
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress httpx informational printout
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)

# Suppress httpcore informational printout
httpcore_logger = logging.getLogger("httpcore")
httpcore_logger.setLevel(logging.WARNING)

r = redis.Redis(
    host='127.0.0.1',
    port=6379,
    charset="utf-8",
    decode_responses=True,
    db=6
)

TMDB_AUTH_TOKEN = os.getenv("TMDB_AUTH_TOKEN")

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_AUTH_TOKEN}"
}

async def requestData(client, imdbId, headers, limiter):
    async with limiter:
        try:
            url = f"https://api.themoviedb.org/3/movie/{imdbId}/credits?language=en-US"

            response = await client.get(url, headers=headers)

            if response.status_code == 200:
                r.set(imdbId, json.dumps(response.json()))            
        
        except httpx.TimeoutException as e:
            logger.error(f"Request to {url} timed out :(", exc_info=True)
        except httpx.ConnectError as e:
            logger.error(f"Connect Error: {e} :(", exc_info=True)
        except httpx.ReadError as e:
            logger.error(f"Read Error: {e}", exc_info=True)
        except httpx.PoolTimeout as e:
            logger.error(f"Timeout Error: {e}", exc_info=True)
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP Status Error: {e}", exc_info=True)
        except httpx.RequestError as e:
            logger.error(f"Request Error: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)


async def processWindow(urls: list):
    rateLimit = AsyncLimiter(45,1) # TMDB has an implied rate limit of 50 per second, so limit to just below that

    async with httpx.AsyncClient() as client:
        tasks = [requestData(client, url, headers, rateLimit) for url in urls]
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            await task
    

# Return list of unprocessed Ids
idFilePath = '../data/allIds.json'
with open(idFilePath) as f:
    allIds = list(json.load(f))

allIds = list(set(allIds))

print(f"NUMBER OF RAW IDS: {len(allIds)}")

# Return redis keys to filter out what is in redis
redisKeys = set(r.keys('*'))
logger.info(f"ORIGINALLY STARTING WITH: {len(redisKeys)} KEYS")

newIds = [u for u in allIds if u not in redisKeys]
logger.info(f"WE GET TO MAKE {len(allIds) - len(newIds)} REDIS KEYS")

# Create batches of 5000
urlsWindowed = createSlidingWindows(l = newIds, windowSize = 5000, overlap = 0)

print(f"LOOKING FOR: {len(newIds)} NEW MOVIES")
print(f"THEY LOOK LIKE THIS: {newIds[0]}")
print(f"WE'VE GOT {len(urlsWindowed)} WINDOWS WITH AN AVG LENGTH OF {round(len(newIds)/len(urlsWindowed), 2)}")

async def main():
    for i, urlWindow in enumerate(urlsWindowed):
        print(f"RUNNING WINDOW: {i}/{len(urlsWindowed)}")
        await processWindow(urlWindow)

asyncio.run(main())
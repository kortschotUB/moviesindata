import sys
sys.path.append('../library')
from core import createSlidingWindows, exceptionOutput
from imageProcessing import getHeadshot, cropCircle, extractFaces
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


TMDB_AUTH_TOKEN = os.getenv("TMDB_AUTH_TOKEN")

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {TMDB_AUTH_TOKEN}"
}

async def requestData(personId, headers, limiter):
    async with limiter:
        try:
            responseHeadshot = getHeadshot(personId, headers, 1)  # Call directly if not a coroutine
            if responseHeadshot is not None and responseHeadshot == 200:
                responseFaces = extractFaces(f"../data/headshots/{personId}/{personId}_0.jpg", makePretty=True)  # Call directly if not a coroutine
                if not isinstance(responseFaces, int):
                    raise TypeError("extractFaces did not return an int")
            else:
                responseFaces = None
        except Exception as e:
            print('SHIT BALLS')
            print(exceptionOutput(e))

    if responseHeadshot == 200 & responseFaces == 200:
        return True
    else:
        return False
    


async def processWindow(personIds: list):
    rateLimit = AsyncLimiter(48,1) # TMDB has an implied rate limit of 50 per second, so limit to just below that

    async with httpx.AsyncClient() as client:
        tasks = [requestData(personId, headers, rateLimit) for personId in personIds]
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            await task
    

# Return list of unprocessed Ids
with open('../assets/nonIPIds.json') as f:
    allIds = json.load(f)

print(f"NUMBER OF RAW IDS: {len(allIds)}")

newIds = [i for i in allIds if ((not os.path.exists(f'../data/headshots/{i}')) or ('00_faceExtracted.png' not in os.listdir(f'../data/headshots/{i}')))]

print(f"NUMBER OF NEW IDS: {len(newIds)}")

# Create batches of 5000
urlsWindowed = createSlidingWindows(l = newIds, windowSize = 5000, overlap = 0)

print(f"LOOKING FOR: {len(newIds)} NEW IDS")
print(f"THEY LOOK LIKE THIS: {newIds[0]}")
print(f"WE'VE GOT {len(urlsWindowed)} WINDOWS WITH AN AVG LENGTH OF {round(len(newIds)/len(urlsWindowed), 2)}")

async def main():
    for i, urlWindow in enumerate(urlsWindowed):
        print(f"RUNNING WINDOW: {i}/{len(urlsWindowed)}")
        await processWindow(urlWindow)

asyncio.run(main())
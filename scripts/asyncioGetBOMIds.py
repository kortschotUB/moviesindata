import modin.pandas as md
import sys
sys.path.append('../library')
from core import createSlidingWindows

from tqdm.asyncio import tqdm
import httpx
import asyncio
from aiolimiter import AsyncLimiter
import logging
import redis
from bs4 import BeautifulSoup
import user_agent

# Configure logging --> This is configured to optimize for tqdm progress. Comment out if there are still unknown errors
logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Suppress httpx informational printout
httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)

# Suppress httpcore informational printout
httpcore_logger = logging.getLogger("httpcore")
httpcore_logger.setLevel(logging.WARNING)

# Establish connection to local redis server
r = redis.Redis(
    host='127.0.0.1',
    port=6379,
    charset="utf-8",
    decode_responses=True,
    db=1
)

async def requestData(client, imdbId, limiter, iteration):
    async with limiter:
        try:
            if iteration < 600: # we already got 302s on everythin up to 600ish
                r.set(imdbId, "unknown")
                return
                        
            url = f"https://www.boxofficemojo.com/title/{imdbId}/?ref_=bo_se_r_1"
            # Rotate headers
            headers = {'User-Agent': user_agent.generate_user_agent()}
            response = await client.get(url, headers=headers)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                soupStr = str(soup)
                bomId = soupStr.split('href="/release/')[1].split('/weekend')[0] #bs4 string search for movies that we have box office info about
                r.set(imdbId, bomId)

            elif response.status_code == 302: # This occurs when there isn't any box office information
                r.set(imdbId, "unknown")

        
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


async def processWindow(urls: list, iteration):
    rateLimit = AsyncLimiter(100,1) # 100 / second

    async with httpx.AsyncClient() as client:
        tasks = [requestData(client, url, rateLimit, iteration) for url in urls]
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            await task
    
allMovieDf = md.read_csv('../data/tmdbDetails.csv')

imdbIdsRaw = allMovieDf['imdb_id'].unique()
        
print(f"NUMBER OF imdbIds : {len(imdbIdsRaw)}")

# Return redis keys to filter out what is in redis
redisKeys = set(r.keys('*'))
print(f"ORIGINALLY STARTING WITH: {len(redisKeys)} KEYS")

imdbIds = [u for u in imdbIdsRaw if u not in redisKeys]
print(f"WE GET TO MAKE {len(imdbIdsRaw) - len(imdbIds)} REDIS KEYS")

# Create batches of 5000
idsWindowed = createSlidingWindows(l = imdbIds, windowSize = 500, overlap = 0)

print(f"LOOKING FOR: {len(imdbIds)} NEW IDs")
print(f"THEY LOOK LIKE THIS: {imdbIds[0]}")
print(f"WE'VE GOT {len(idsWindowed)} WINDOWS WITH AN AVG LENGTH OF {round(len(imdbIds)/len(idsWindowed), 2)}")

async def main():
    for i, idWindow in enumerate(idsWindowed):
        print(f"RUNNING WINDOW: {i}/{len(idsWindowed)}")
        await processWindow(idWindow, i)

if __name__ == '__main__':
    asyncio.run(main())
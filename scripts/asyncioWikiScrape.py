import modin.pandas as md
import sys
sys.path.append('../library')
from core import createSlidingWindows, exceptionOutput
import urllib.parse
from tqdm.asyncio import tqdm
import httpx
import asyncio
from aiolimiter import AsyncLimiter
import logging
import redis
from bs4 import BeautifulSoup
import user_agent
from SPARQLWrapper import SPARQLWrapper, JSON
import wikipediaapi
import json
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
r9 = redis.Redis(
    host='127.0.0.1',
    port=6379,
    charset="utf-8",
    decode_responses=True,
    db=9
)

async def returnWikiURL(imdbId: str) -> str:
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    queryString = """
    SELECT ?wppage WHERE {
    ?subject wdt:P345 'IMDB-ID' . 
        ?wppage schema:about ?subject .
        FILTER(contains(str(?wppage),'//en.wikipedia'))
    }
    """
    queryString = queryString.replace("IMDB-ID", imdbId)
    sparql.setQuery(queryString)
    sparql.setReturnFormat(JSON)

    try:
        results = sparql.query().convert()
        requestGood = True
    except Exception as e:
        results = str(e)
        requestGood = False

    if requestGood:
        return results['results']['bindings'][0]['wppage']['value'
    else:
        return ""

async def requestData(wiki_wiki, imdbId, limiter, iteration):
    async with limiter:
        try:                        
            wikiUrl = await returnWikiURL(imdbId)
            decodedUrl = urllib.parse.unquote(wikiUrl)
            wikiPage = wiki_wiki.page(decodedUrl.split('/')[-1])
            if len(wikiPage.sections) == 0:
                print(imdbId, wikiUrl)
                return

            plotText = str(wikiPage.sections[0]) # Plot tends to be the first section for movies... will likely need some error handling here + maybe a check to see if title is called plot
            
            r9.set(imdbId, json.dumps(plotText))
        
        except Exception as e:
            exceptionOutput(e)
            pass


async def processWindow(wiki_wiki, imdbIds: list, iteration):
    rateLimit = AsyncLimiter(100,1) # n / second

    async with httpx.AsyncClient() as client:
        tasks = [requestData(wiki_wiki, imdbId, rateLimit, iteration) for imdbId in imdbIds]
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            await task

async def main():    

    wiki_wiki = wikipediaapi.Wikipedia('Project: Testproject', 'en')

    with open('../data/horrorIds.json') as f:
        imdbIdsRaw = json.load(f)
            
    print(f"NUMBER OF imdbIds : {len(imdbIdsRaw)}")

    # Return redis keys to filter out what is in redis
    redisKeys = set(r9.keys('*'))
    print(f"ORIGINALLY STARTING WITH: {len(redisKeys)} KEYS")

    imdbIds = [u for u in imdbIdsRaw if u not in redisKeys]
    print(f"WE GET TO MAKE {len(imdbIdsRaw) - len(imdbIds)} REDIS KEYS")

    # Create batches of 5000
    idsWindowed = createSlidingWindows(l = imdbIds, windowSize = 500, overlap = 0)

    print(f"LOOKING FOR: {len(imdbIds)} NEW IDs")
    print(f"THEY LOOK LIKE THIS: {imdbIds[0]}")
    print(f"WE'VE GOT {len(idsWindowed)} WINDOWS WITH AN AVG LENGTH OF {round(len(imdbIds)/len(idsWindowed), 2)}")
    
    for i, idWindow in enumerate(idsWindowed):
        print(f"RUNNING WINDOW: {i}/{len(idsWindowed)}")
        await processWindow(wiki_wiki, idWindow, i)

if __name__ == '__main__':
    asyncio.run(main())
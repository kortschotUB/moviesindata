import sys
sys.path.append('../library')
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
logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
            soup = BeautifulSoup(response.text, 'html.parser')

            rating = ast.literal_eval(str(soup).split('ratingsSummary":')[1].split('}')[0] + '}')['aggregateRating']
                
            # Create primary key for live redis caching
            r5.set(imdbId, rating)
            return

        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            return

async def findRating(allIds: list):
    rateLimit = AsyncLimiter(5,1) # 100 / second

    async with httpx.AsyncClient() as client:
        tasks = [requestData(client, imdbId, rateLimit) for imdbId in allIds]
        for task in asyncio.as_completed(tasks):
            await task

# This is just a temporary list, we'll
ids = ['tt0107614', 'tt0129387', 'tt0110475', 'tt3783958', 'tt13364790',
    'tt1431045', 'tt1119646', 'tt0109830', 'tt0795421', 'tt0138097',
    'tt0118799', 'tt0243155', 'tt1045658', 'tt1637725', 'tt0108160',
    'tt1284575', 'tt0092644', 'tt2848292', 'tt0087332', 'tt0094898',
    'tt0252866', 'tt0145660', 'tt0458352', 'tt1753383', 'tt0453451',
    'tt4651520', 'tt0104714', 'tt0105793', 'tt4701660', 'tt0097733',
    'tt1478338', 'tt1033575', 'tt0125439', 'tt7752454', 'tt0111070',
    'tt26047818', 'tt0094889', 'tt0829482', 'tt0112431', 'tt1142988',
    'tt14109724', 'tt0084805', 'tt0364725', 'tt0102510', 'tt0096874',
    'tt0120484', 'tt3104988', 'tt1041829', 'tt5503686', 'tt0119738',
    'tt0404364', 'tt1137470', 'tt1446147', 'tt1034320', 'tt1107860',
    'tt11908172', 'tt1737237', 'tt0157262', 'tt0387808', 'tt1274300',
    'tt3038708', 'tt0271946', 'tt2222042', 'tt0082525', 'tt0091757',
    'tt0118001', 'tt1132449', 'tt9686708', 'tt0120199', 'tt1182924',
    'tt10310140', 'tt0141399', 'tt0266747', 'tt13923456', 'tt0108187',
    'tt0117768', 'tt0120716', 'tt0110074', 'tt0083254', 'tt0110197',
    'tt0065611', 'tt0111094', 'tt0315824', 'tt0437954', 'tt0160236',
    'tt2229848', 'tt0266452', 'tt22866358', 'tt1974420', 'tt0114614',
    'tt0138510', 'tt0096764', 'tt0116165', 'tt0120645', 'tt0093405',
    'tt0145734', 'tt1053810', 'tt0096486', 'tt0120701', 'tt0119053']

async def main():
    await findRating(ids)

if __name__ == '__main__':
    asyncio.run(main())
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
    db=8
)

async def requestData(client, link, limiter):
    async with limiter:
        try:

            franchiseData = {}


            boxOfficeBaseUrl = os.getenv("BOX_OFFICE_BASE_URL")
            url = f'{boxOfficeBaseUrl}{link}'
            headers = {'User-Agent': user_agent.generate_user_agent()}
            response = await client.get(url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')

            # tables = soup.find_all('table', id='box_office_weekly_table')
            table = soup.find('table', id='franchise_movies_overview')

            if table == None:
                return

            franchiseTableData = []

            for row in table.find_all('tr'):
                rowData = []
                cols = row.find_all('td')
                for col in cols[1:]:
                    movieLink = col.find('a')
                    href = movieLink['href'] if movieLink else None
                    if movieLink:
                        franchiseTableData.append(href)

            franchiseName = link.split('franchise/')[1]

            franchiseData[franchiseName] = {}

            for movie in franchiseTableData:
                try:
                
                    if '-(' in movie:
                        movieName = movie.split('/movie/')[1].split('-(')[0].replace('-', ' ')
                    else:
                        movieName = movie.split('/movie/')[1].split('#tab')

                    movieURL = f"{boxOfficeBaseUrl}{movie}#tab=cast-and-crew"
                    
                    headers = {'User-Agent': user_agent.generate_user_agent()}
                    response = await client.get(movieURL, headers=headers)

                    if response.status_code != 200:
                        continue

                    soup = BeautifulSoup(response.content, 'html.parser')

                    

                    tables = soup.find_all('table')

                    people = []
                    for table in tables:
                        links = table.find_all('a', href=True)
                        for personLink in links:
                            href = personLink['href']
                            if 'person' in href:
                                people.append(' '.join(href.split('-')[1:]))

                    franchiseData[franchiseName][movie] = {
                        'url': movieURL,
                        'title':movieName,
                        'people':people
                    }
                except Exception as e:
                    exceptionOutput(e)
                    pass

            r.set(franchiseName, json.dumps(franchiseData))
        except Exception as e:
            exceptionOutput(e)
            pass



async def processWindow(links: list):
    rateLimit = AsyncLimiter(5,1) # 100 / second
    
    timeout = httpx.Timeout(10.0, read=30.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        tasks = [requestData(client, link, rateLimit) for link in links]
        
        for task in asyncio.as_completed(tasks):
            await task



async def main():
    url = os.getenv("FRANCHISE_URL") + '#franchise_overview=l1000:od3'
    headers = {'User-Agent': user_agent.generate_user_agent()}
    response = httpx.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # tables = soup.find_all('table', id='box_office_weekly_table')
    table = soup.find('table', id='franchise_overview')

    tableData = []

    for row in table.find_all('tr'):
        rowData = []
        cols = row.find_all('td')
        for col in cols:
            link = col.find('a')
            href = link['href'] if link else None
            if link:
                tableData.append(href)
        tableData.append(rowData)

    tableDataFiltered = [t for t in tableData if t!=[]]

    print(f"LOOKING AT {len(tableDataFiltered)} IDS")

    idsWindowed = createSlidingWindows(l = tableDataFiltered, windowSize = 50, overlap = 0)


    for i, idWindow in enumerate(idsWindowed, start=1):
        print(f"RUNNING WINDOW: {i}/{len(idsWindowed)}")
        await processWindow(idWindow)


if __name__ == '__main__':
    asyncio.run(main())

import pandas as pd
import sys
sys.path.append('../library')
from core import *

from datetime import datetime
from tqdm.asyncio import tqdm
import httpx
import asyncio
from aiolimiter import AsyncLimiter
import json

# Return list of unprocessed Ids
idFilePath = '../assets/unseenIds.json'
with open(idFilePath) as f:
    urls = json.load(f)

print(f"LOOKING FOR: {len(urls)}")
print(f"THEY LOOK LIKE THIS: {urls[0]}")

async def requestData(client, url, limiter):
    async with limiter:
        try:
            response = await client.get(url)
        
            if response.status_code == 200:
                return response.json()
        
        except httpx.TimeoutException as e:
            # print(f"Request to {url} timed out :(")
            pass
        except httpx.ConnectError as e:
            # print(f"Connect Error: {e} :(")
            pass


async def main():
    rateLimit = AsyncLimiter(30,1)

    async with httpx.AsyncClient() as client:
        tasks = []
        for url in urls:
            tasks.append(requestData(client, url, rateLimit))
        
        # data = await asyncio.gather(*tasks)
                # Use tqdm to track progress
        data = []
        for task in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            result = await task
            data.append(result)
    
    return data    
    
# if __name__ == '__main__':
results = asyncio.run(main())

allData = [d for d in results if isinstance(d, dict)]

saveTime = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

dataDf = pd.DataFrame.from_dict(allData)

dataDf.to_csv(f'../data/movieDetails_{saveTime}.csv')
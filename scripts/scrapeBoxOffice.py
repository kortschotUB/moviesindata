import requests
from dotenv import load_dotenv
load_dotenv(dotenv_path='../assets/.env')
import pandas as pd
import os
import sys
sys.path.append('../library')
from core import *
import json
import numpy as np
from datetime import datetime
import time
import requests
import json
from multiprocessing import Pool

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_AUTH_TOKEN = os.getenv("TMDB_AUTH_TOKEN")

def scrapeUrl(url: str = ''):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            filmData = json.loads(response.text)
            return filmData

        if response.status_code == 429:
            time.sleep(5)
    except:
        pass

def scrapeParallel(url):
    result = scrapeUrl(url)
    return result

if __name__ == '__main__':
    startI =  200000
    endI   = 1200000

    urlList = [f'https://api.themoviedb.org/3/movie/{i}?api_key={TMDB_API_KEY}&language=en-US' for i in np.arange(startI, endI)]

    numProcesses = 4

    # Using Pool for parallel execution
    with Pool(numProcesses) as pool:
        allData = pool.map(scrapeParallel, urlList)

    allData = [d for d in allData if isinstance(d, dict)]

    saveTime = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

    dataDf = pd.DataFrame.from_dict(allData)

    dataDf.to_csv(f'../data/movieDetails_{saveTime}.csv')
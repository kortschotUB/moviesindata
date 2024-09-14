from multiprocessing import Pool
import os
import redis
import json
import pandas as pd
import numpy as np
import sys
sys.path.append('../library/')
from core import extractBetween, extractElementsInOrder, exceptionOutput
from time import time
from uuid import uuid1
import arrow
from tqdm import tqdm
from collections import defaultdict
from functools import partial

def tripletSearchWithYear(target, candidateTitles, candidateReleaseDates, candidateIds):
    try:
        """ Triplet search
        
            - Uses a chunk based search algorithm to search for a target against a list of candidates
            - This includes a year for final filtering
        """
        targetRelease = np.datetime64(pd.to_datetime(target['dateDt'], utc=True))
        targetId = target['uuid']
        targetTitle = target['title'].lower()

        matchTitleIdx = np.flatnonzero(candidateTitles == targetTitle)

        # Calculate the absolute difference in days
        candidateReleaseDates = candidateReleaseDates.astype('datetime64[D]')
        diff = np.abs(candidateReleaseDates - targetRelease)

        # Check if the difference is within 7 days
        within14 = diff <= np.timedelta64(14, 'D')
        matchDateIdx = np.where(within14)[0]

        # Find overlap
        overlapIdx = np.intersect1d(matchTitleIdx, matchDateIdx)

        if len(overlapIdx) == 1:
            print('yassss')
            print(overlapIdx)
            return {targetId: candidateIds[overlapIdx[0]]}
        
        # If we don't find an exact match... 
        
        # scores = {}
        
        # def getChunks(str, chunkSize=3):
        #     chunks = [] 
        #     for i in range(len(str)):
        #         chunk = str[i:i+chunkSize]
        #         if len(chunk)==chunkSize:
        #             chunks.append(chunk)
            
        #     return chunks

        # targetChunks = getChunks(targetTitle)

        # # print(len(candidateDict))
        # targetTitleLen = len(targetTitle)
        # targetTitleSet = set(targetTitle)

        # idxsFiltered = [j for j, i in enumerate(candidateIds) 
        #             if (
        #                 abs (targetRelease - candidateReleaseDates[j]).total_seconds() < (400 * 24 * 3600) and
        #                 abs (targetTitleLen - len(candidateTitles[j])) < 2 and
        #                 len(targetTitleSet & set(candidateTitles[j])) > 0
        #             )]
        
        # titlesFiltered = [candidateTitles[i] for i in idxsFiltered]
        # idsFiltered = [candidateIds[i] for i in idxsFiltered]
        # releaseDatesFiltered = [candidateReleaseDates[i] for i in idxsFiltered]
        
        # # print(len(candidatesFiltered))

        # for i, imdbId in enumerate(idsFiltered):
        #     candidateChunks = getChunks(titlesFiltered[i])
            
        #     intersection1 = [chunk for chunk in candidateChunks if chunk in targetChunks]
        #     intersection2 = [chunk for chunk in targetChunks if chunk in candidateChunks]
            
        #     if ((len(intersection1) > 0) & (len(intersection2))):
        #         intersectionScore = (len(intersection1) / len(candidateChunks)) + (len(intersection2) / len(targetChunks))
        #     else:
        #         intersectionScore = 0

        #     if intersectionScore > 1: # Max score is 2
        #         scores[imdbId] = intersectionScore


        # if len(scores) == 0:
        #     return {targetId: 'NO CANDIDATE WAS FOUND'}
        
        # maxSim = max(scores.values())
        
        # maxIds = [i for i in scores.keys() if scores[i] == maxSim]
        
        # if len(maxIds) == 1:
        #     bestMatch = maxIds[0]
        
        # elif len(maxIds) > 1: # This is for disambiguation of similar titles
        #     deltas = {i: abs((targetRelease - arrow.get(candidateDict[i]['release_date'])).total_seconds()) for i in maxIds if candidateDict[i]['release_date'] == candidateDict[i]['release_date']}
        #     minDelta = min(deltas, key = deltas.get)
            
        #     bestMatch = minDelta
        # else:
        #     bestMatch = 'NO CANDIDATE WAS FOUND'

        # # Write to redis
        # # r4.set(bestMatch, json.dumps(target))

        # return  {targetId: bestMatch}
    except Exception as e:
        print(exceptionOutput(e))
        print(target)


if __name__ == '__main__':
    r = redis.Redis(
        host='localhost',
        port=6379,
        charset="utf-8",
        decode_responses=True,
        db = 3
    )

    # TARGET DF
    dates = r.keys()
    vals  = r.mget(dates)

    valsJson = [el[1:] for el in [json.loads(e) for e in vals]]

    columns = ['rank','previous','title','distributor','weekGross','pctLastWeek','numberOfTheaters','numberOfTheatersChange','perTheaterAvg','totalGross','weeksInRelease']

    allWeeks = []
    for i, week in enumerate(tqdm(valsJson)):
        for movie in week:
            if all([v == '' for v in movie]) | (movie[0] == ''):
                continue

            movieDict = dict(zip(columns, movie))
            movieDict['date'] = dates[i]
            allWeeks.append(movieDict)

    df = pd.DataFrame.from_dict(allWeeks)

    df['dateDt'] = pd.to_datetime(df['date'], utc=True)

    for col in ['weekGross','pctLastWeek','numberOfTheaters','perTheaterAvg','totalGross']:
        df[col] = df[col].str.replace('$','', regex=False)\
                                        .str.replace(',','',regex=False)\
                                        .str.replace('%','',regex=False)\
                                        .str.replace('<','',regex=False)\
                                        .str.replace('(v)', '', regex=False)\
                                        .replace('-', np.nan)\
                                        .replace('', np.nan)\
                                        .replace('n/c', np.nan)\
                                        .astype(float)

    df.sort_values(by=['dateDt', 'weekGross'], ascending=[False, False], inplace=True)
    
    # This step is required to infer the release date
    df.drop_duplicates(subset=['title','distributor'], keep='last')

    df['uuid'] = df.apply(lambda x: str(uuid1()), axis=1)
    targetDict = df[['uuid','title','dateDt']].to_dict('records')


    # CANDIDATE DF
    tmdbDf = pd.read_csv('../data/tmdbDetails.csv')
    relDf = tmdbDf[['title', 'release_date','imdb_id']]
    relDf['release_date'] = pd.to_datetime(relDf['release_date'], utc=True)
    relDf.drop_duplicates(subset='imdb_id', keep='last', inplace=True)
    relDf.set_index('imdb_id', inplace=True)
    relDf.dropna(subset='release_date', inplace=True)
    candidateDict = relDf.to_dict('index')


    # Set up threading
    cores = os.cpu_count()
    pool = Pool(cores)

    # Form lists for faster processing test
    candidateTitles = np.array([c['title'].lower() for c in candidateDict.values()])
    candidateIds = np.array(list(candidateDict.keys()))
    candidateReleaseDates = np.array([c['release_date'] for c in candidateDict.values()])

    tripletSearchWithYearPartial = partial(tripletSearchWithYear, candidateTitles = candidateTitles, candidateReleaseDates = candidateReleaseDates, candidateIds = candidateIds)
    
    t1 = time()
    cutoff = 500
    results = list(tqdm(pool.imap(tripletSearchWithYearPartial, targetDict[:cutoff]), total=len(targetDict[:cutoff])))
    t2 = time()

    print(f"Execution Time: {t2-t1}")

    resultDict = {k: v for d in results for k, v in d.items()}

    df['imdbId'] = df['uuid'].map(resultDict)

    pool.close()
    pool.join()

    df.to_csv("../data/weeklyBoxOffice.csv")
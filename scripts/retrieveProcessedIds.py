from dotenv import load_dotenv
load_dotenv(dotenv_path='../.env')
import os
import json
import modin.pandas as md

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
TMDB_AUTH_TOKEN = os.getenv("TMDB_AUTH_TOKEN")



df = md.read_csv('../data/tmdbDetails.csv')
df.drop_duplicates(inplace=True, keep='last')

# Generate list of unprocessed Ids
idFilePath = '../assets/allIds.json'
with open(idFilePath) as f:
    movieIds = json.load(f)

rawIds = [i['id'] for i in movieIds]

print(f"TOTAL IDS: {len(rawIds)}")

foundIds = set(df['id'].unique())
newIds = [i for i in rawIds if i not in foundIds]

print(f"NEW IDS: {len(newIds)}")

urls = [f'https://api.themoviedb.org/3/movie/{i}?api_key={TMDB_API_KEY}&language=en-US' for i in rawIds]

savePath = '../assets/unseenIds.json' 
with open(savePath, 'w') as f:
    json.dump(urls, f)
import inflect
import json
import numpy as np
import itertools
from dotenv import load_dotenv
import traceback
load_dotenv(dotenv_path='../.env')
import sys
import string as stringLib
import math
import re
from typing import Generator

def exceptionOutput(e, p=True, tb=False, **kwargs):
    """ Exception Output
    
        - Pretty printing function for an exception
    
    """
    if tb:
        errorLog = traceback.format_exception(type(e), e, e.__traceback__)    
    else:
        errorLog = 'Error on line {}'.format(sys.exc_info()[-1].tb_lineno), str(type(e).__name__), str(e)
    
    if p:
        print(" || ".join(errorLog))
    
    return " || ".join(errorLog)

def np_encoder(object):
    """ Numpy Encoder
        - Outcome: Provided as a default argument for json.dumps and allows for json encoding of numpy arrays
        - Method: Converts numpy array to its values (?)
        - Purpose: Allows more flexibility in the python exit function
    """
    if isinstance(object, np.generic):
        return object.item()

def prettyPrint(jsonObj: dict) -> None:
    """ Pretty Print 
        - Pretty prints a json object    
    """

    print(json.dumps(jsonObj, indent=4, sort_keys=False, default=np_encoder), end='')

def titleify(string: str) -> str:
    """ Titleify
        - Converts a string to a title
        - e.g., you were sad today -> You Were Sad Today
    """
    words = string.split(' ')
    title = ''

    for word in words:
        cap = word[0:1].upper()
        rest = word[1:]
        full = cap + rest
        title += full + ' '

    return title.rstrip()

def sentenceify(string: str) -> str:
    """ Sentenceify
        - Converts a string to a sentence
        - e.g., you were sad today -> You were sad today.
    """
    start = string[0].upper()
    middle = string[1:-1]
    end = string[-1]

    if end in stringLib.punctuation:
        fin = end
    else:
        fin = end + '.'

    sentence = start + middle + fin

    return sentence


def createSlidingWindows(l=[], windowSize=5, overlap=0):
    """ Create Sliding Windows
        - Divides a long list into a series of sublists in order to process things in batches typically
    """
    step = windowSize - overlap
    nWindows = math.ceil((len(l) - overlap) / step)

    outputL = []

    startDex = 0
    for i in range(nWindows):
        end = min(startDex + windowSize, len(l))
        sample = l[startDex:end]
        
        if len(sample) > 0:
            outputL.append(sample)
            startDex += step
        else:
            break
    
    return outputL


def extractBetween(text: str, start: str, end: str):
    """ Extract Between
        - Performs regex on a string to find all elements between two sustrs
    """
    pattern = re.escape(start) + r'(.*?)' + re.escape(end)
    match = re.search(pattern, text)
    return match.group(1) if match else None


def extractElementsInOrder(text, elements):
    """ Extract Elements in Order
        - Returns a list of elements in the order that they appear in the original text
        - E.g., if string is "hello how are you doing" and you pass in ["doing", "are", "sunshine"] --> ["are", "doing"]
    """
    pattern = re.compile('|'.join(map(re.escape, elements)))
    matches = pattern.findall(text)
    return matches


#| export
def flattenWithGenerator(lst: list) -> Generator:
    """ Flatten With Generator
        - Flattens a list of arbitrary depth
        - Yields a generator
    """
    
    for item in lst:
        if isinstance(item, list):
            yield from flattenWithGenerator(item)
        else:
            yield item

def tripletSearchWithYear(target: list, candidates: list) -> str:
    """ Triplet search
        - Uses a chunk based search algorithm to search for a target against a list of candidates
        - This includes a year for final filtering:
            - target and candidates take the form of list of dicts, each with keys title and year
            e.g., [{'title':'Drive','year':2011}]
    """
    scores = {}

    def getChunks(str, chunkSize=3):
        chunks = [] 
        for i in range(len(str)):
            chunk = str[i:i+chunkSize]
            if len(chunk)==chunkSize:
                chunks.append(chunk)
        
        return chunks

    targetChunks = getChunks(target['title'])

    for candidate in candidates:
        candidateChunks = getChunks(candidate['title'].lower())
        
        intersection = [chunk for chunk in candidateChunks if chunk in targetChunks]
        intersectionScore = len(intersection)/len(candidateChunks)
        
        if intersectionScore >= .5:
            scores[candidate['title']] = intersectionScore

            

    if len(scores) > 0:
        return max(scores, key=scores.get)
    else:
        return 'NO CANDIDATE WAS FOUND'
    
def getLengthOfDict(d):
    """Get Length of Dict
        - returns the summative length of all elements in a nested dict
    """
    if isinstance(d, dict):
        return sum(getLengthOfDict(v) for v in d.values())
    elif isinstance(d, list):
        return sum(getLengthOfDict(v) for v in d)
    else:
        return 1
    
# Function to make all words in a string singular
def makeSingular(text):
    p = inflect.engine()
    words = text.split()
    singular_words = [p.singular_noun(word) if p.singular_noun(word) else word for word in words]
    return ' '.join(singular_words)
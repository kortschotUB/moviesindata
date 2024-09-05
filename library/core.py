
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

def createSlidingWindows(l=[], windowSize=5, startDex=0, overlap=0, **kwargs):
    """ Create Sliding Windows
    
        - Divides a long list into a series of sublists in order to process things in batches typically
    
    """
    nWindows = math.ceil(len(l)/windowSize)

    outputL = []

    for i in range(nWindows):
        end = min(startDex + windowSize, len(l))
        sample = l[startDex:end]
        
        if len(sample) > 0:
            outputL.append(sample)
            startDex+=windowSize
        else:
            break
    
    return outputL


def extractBetween(text: str, start: str, end: str):
    # Use a regex pattern to extract everything between start and end
    pattern = re.escape(start) + r'(.*?)' + re.escape(end)
    match = re.search(pattern, text)
    return match.group(1) if match else None


def extractElementsInOrder(text, elements):
    # Create a regex pattern to match any of the elements
    pattern = re.compile('|'.join(map(re.escape, elements)))
    # Find all matches in the text
    matches = pattern.findall(text)
    return matches
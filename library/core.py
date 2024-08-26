
import json
import numpy as np
from sklearn import preprocessing
import itertools
import statsmodels.api as sm
from dotenv import load_dotenv
import traceback
load_dotenv(dotenv_path='../.env')
import sys

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

def linearModelGeneral(X: list, y: list, degrees = [1,2,3]) -> dict:
    """ Linear Model General
    - Takes a list of x and y
    - Loops through the degrees specified in the degrees argument 
    - Returns the result that has the highest r2 value
    """
    resultsDict = {}

    # Reshape X if one-d
    if(len(X.shape)<2):
        X = X[:,np.newaxis]

    for degree in degrees:
        resultsDictTemp = {}
        polynomial_features = preprocessing.PolynomialFeatures(degree=degree)

        X_poly = polynomial_features.fit_transform(X)
        model = sm.OLS(y, X_poly).fit()
        ypred = np.array(model.predict(X_poly))

        resultsDictTemp['X'] = list(itertools.chain(*X))
        resultsDictTemp['p'] = model.f_pvalue
        resultsDictTemp['F'] = model.fvalue
        resultsDictTemp['r2'] = model.rsquared
        resultsDictTemp['params'] = model.params
        resultsDictTemp['ypred'] = list(ypred)
        resultsDictTemp['degree'] = degree

        if(len(resultsDict)==0):
            resultsDict = resultsDictTemp
        else:
            if(resultsDictTemp['r2']>resultsDict['r2']):
                resultsDict = resultsDictTemp

    return resultsDict
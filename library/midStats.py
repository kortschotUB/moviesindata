import pandas as pd
import numpy as np
from sklearn import preprocessing
import statsmodels.api as sm
import itertools

def dropNumericalOutliers(
    x: pd.Series, # PD series to drop outliers in
    keepNull: bool = True, # Fill dropped values with null or drop from series
    low: float = .01, # Low end for outlier
    high: float = .99 # High end for outlier
) -> list:
    """ Drop Numerical Outliers
    
        - Drops all fo the numerical outliers from a pandas series
        - Provides the option to keep null values or drop them
            - Should keep if want to preserve the original index
            - If you select keep, it will replace the numerical outliers with an np.nan
    
    """

    try:
        x=np.array(x) #numpyify the series

        xNum = [val for val in x if ~np.isnan(val)] # select non null vals from x
        ql = np.quantile(xNum, low) # get quantile low
        qh = np.quantile(xNum, high) # get quantile high

        # If we want to keep null vals but remove the numerical outliers
        # This is done to keep lengths of values the same for later interpolation
        if(keepNull):
            stripped = []
            for val in list(x):
                if((val>=ql) & (val<=qh)):#valid condition
                    stripped.append(val)
                elif(((val<ql)|(val>qh))&(~np.isnan(val))):#outlier condition
                    stripped.append(np.nan)
                else:#nan condition
                    stripped.append(np.nan)

            if(len(x) != len(stripped)):
                return "Error: Some values lost during dropna and not replaced with np.nan. Will cause issues with subsequent analyses."
        else:
            stripped = x[x.between(ql, qh)] # without outliers & nan vals

        return stripped
    except:
        return x


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
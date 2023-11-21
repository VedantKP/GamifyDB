import pandas as pd
import numpy as np

imdbSchema = ['name','url','year','rating','votes','plot']
salesSchema = ['Genre','NA_Sales','EU_Sales','JP_Sales','Other_Sales','Global_Sales']

"""
Function reads and returns CSV data as a dataframe, filtering columns as per input schema.
"""
def readCsv(path=None,schema=None):
    df = pd.DataFrame()
    if path and schema:
        df = pd.read_csv(path,usecols=schema)
    elif path:
        df = pd.read_csv(path)
    return df

import pandas as pd
import numpy as np

"""
Function reads and returns CSV data as a dataframe, filtering columns as per input schema.
"""
def readCsv(path=None,schema=None,index_col=None) -> pd.DataFrame:
    df = pd.DataFrame()
    if path and schema and index_col:
        df = pd.read_csv(path,usecols=schema,index_col=index_col)
    elif path and schema:
        df = pd.read_csv(path,usecols=schema)
    elif path:
        df = pd.read_csv(path)
    return df

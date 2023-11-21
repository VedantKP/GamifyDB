import pandas as pd
import numpy as np

imdbSchema = ['name','url','year','rating','votes','plot']
salesSchema = ['Genre','NA_Sales','EU_Sales','JP_Sales','Other_Sales','Global_Sales']

"""
Function reads and returns imdb-videogames.csv data as a dataframe
"""
def readImdb():
    df = pd.read_csv('../datasets/imdb-videogames.csv',usecols=imdbSchema)
    return df

"""
Function reads and returns vg-sales.csv data as a dataframe
"""
def readSales():
    df = pd.read_csv('../datasets/vgsales.csv',usecols=salesSchema)
    return df
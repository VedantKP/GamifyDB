import pandas as pd
import numpy as np
from read_data import readCsv

imdbSchema = ['name','url','year','rating','votes','plot']
salesSchema = ['Name','Year','Genre','NA_Sales','EU_Sales','JP_Sales','Other_Sales','Global_Sales']

'''
Function reads vgsales dataset and converts Year's datatype from float64 to int64.
Return the whole dataframe
'''
def getDataVgSales():
    vgsales_df = readCsv(path='../datasets/vgsales.csv',schema=salesSchema)
    vgsales_df = vgsales_df.fillna(0).astype({'Year':'int64'}) #fillna used to remove null values so that year can be cast to int64 for further comparison
    return vgsales_df

'''
Function performs the following tasks:
1. Reads imdb data from csv file.
2. Removes duplicate records.
3. Matches data with vgsales dataset to return the matched data.
'''
def getDataImdb(names,years):
    imdb_df = readCsv(path='../datasets/imdb-videogames.csv',schema=imdbSchema)
    imdb_df = imdb_df.fillna(0).astype({'year':'int64','name':'string'})
    imdb_df = imdb_df.drop_duplicates('name') #Removes 1089 duplicate records

    subset_df = pd.DataFrame(columns=imdbSchema)
    subset_df = subset_df.astype({'year':'int64','name':'string','rating':'float64'}) #Creates a blank dataframe with same schema as imdb
    
    for name,year in zip(names,years):
        record = imdb_df.loc[(imdb_df['name'] == name) & (imdb_df['year'] == year)] #Extracts records with matching name and year for the game
        if not record.empty:
            subset_df = pd.concat([subset_df,record],axis=0) 
    # print(subset_df)
    return subset_df

'''
Function integrates data from all 4 sources: vgsales, imdb, igdb and rates
1. extract names from vgsales and save as a pandas series
2. extract records for the same names from imdb
3. extract records for the same names from igdb's json data
4. extract records for the same names from rates' json data
'''
def integrate():
    vgsales_df = getDataVgSales()
    names, year = vgsales_df.Name, vgsales_df.Year
    imdb_df = getDataImdb(names,year) #Extract imdb data with matching rows from sales data.
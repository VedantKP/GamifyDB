import pandas as pd
import numpy as np
from read_data import readCsv
import time
import json
import requests

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
#cheapstark = ['gameID','steamAppID','cheapest','cheapestDealID','external','internalName','thumb']
pulled data from games and not List of deals which contains duplicates , handling them would be tough.
need to check which API to request.
need to understand which attributes would be relevant to our global schema.
'''


def getCheapStarkAPI():
    df = getDataVgSales()
    cheapstark =  pd.DataFrame()

    for i in df['Name']:
        #print(i)
        res = requests.get('https://www.cheapshark.com/api/1.0/games?title='+i+'&exact=1')
        data = res.text
    
        #print(data)
    
        df2 = pd.read_json(data)
        cheapstark = pd.concat([cheapstark, df2], ignore_index=True)
    
        time.sleep(3)

    return cheapstark

'''
Function integrates data from all 4 sources: vgsales, imdb, igdb and rates
1. extract names from vgsales and save as a pandas series
2. extract records for the same names from imdb
3. extract records for the same names from igdb's json data
4. extract records for the same names from rates' json data
def integrate():
    vgsales_df = getDataVgSales()
    names, year = vgsales_df.Name, vgsales_df.Year
    imdb_df = getDataImdb(names,year) #Extract imdb data with matching rows from sales data.
'''

"""returns matching dataframes from sales and imdb"""
def getCorrectSalesAndIBDB():
    vgsales_df = getDataVgSales()  # all data "sales"
    names, year = vgsales_df.Name, vgsales_df.Year
    imdb_df = getDataImdb(names, year)  # ibmd with intersection of sales and imdb data
    return vgsales_df, imdb_df


"""Function to calculate Jaccard similarity between two strings"""
def jaccard_similarity(s1, s2):
    set1 = set(s1)
    set2 = set(s2)
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0


"""Function to map attributes from input schemas to the global schema using Jaccard similarity"""
def map_to_global_schema(global_schema, source_df):
    mapping = {}
    for source_attribute in source_df.columns:
        max_similarity = 0
        mapped_attribute = None

        for global_attribute in global_schema:
            similarity = jaccard_similarity(source_attribute.lower(), global_attribute.lower())
            if similarity > max_similarity:
                max_similarity = similarity
                mapped_attribute = global_attribute

        mapping[source_attribute] = mapped_attribute

    return mapping


"""Combine sales and imdb to global table"""
def combine_to_global_table():
    """Define global schema"""
    global_schema = [
        'name', 'rating', 'votes', 'year', 'url', 'plot', 'Genre',
        'NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales'
    ]

    # get both dataframes
    vgsales_df, imdb_df = getCorrectSalesAndIBDB()

    # apply automated mapping algorithm
    mapping_vgsales_to_global = map_to_global_schema(global_schema, vgsales_df)
    mapping_imdb_to_global = map_to_global_schema(global_schema, imdb_df)

    imdb_df.rename(columns=mapping_imdb_to_global, inplace=True)

    # merge both tables to global dataframe
    global_df = pd.merge(vgsales_df.rename(columns=mapping_vgsales_to_global),
                         imdb_df,
                         on=['name', 'year'],  # Merging on 'name' and 'year'
                         how='inner')  # Choose appropriate merge type

    global_df = global_df.reindex(columns=global_schema)

    return global_df

# Calling the function to get the combined global table
combined_global_table = combine_to_global_table()
print(combined_global_table)

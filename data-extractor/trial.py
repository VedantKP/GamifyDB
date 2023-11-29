import pandas as pd
import numpy as np
from read_data import readCsv
import requests
import json
from pandas import json_normalize 

imdbSchema = ['name','url','year','rating','votes','plot']
salesSchema = ['Name','Year','Genre','NA_Sales','EU_Sales','JP_Sales','Other_Sales','Global_Sales']
'''
res = requests.get('https://www.cheapshark.com/api/1.0/deals?storeID=1&upperPrice=15')
response = json.loads(res.text) 
print(response)

Fetch cheapstark API // match the name data with vg sales and fetch only relevant rows.
'''
def getDataVgSales():
    vgsales_df = readCsv(path='../datasets/vgsales.csv',schema=salesSchema)
    vgsales_df = vgsales_df.fillna(0).astype({'Year':'int64'}) #fillna used to remove null values so that year can be cast to int64 for further comparison
    return vgsales_df

df = getDataVgSales()

count = 0

cheapstark_schema = ['gameID','steamAppID','cheapest','cheapestDealID','external','internalName','thumb']

cheapstark =  pd.DataFrame()


for i in df['Name']:
    cheapstark =  pd.DataFrame()
    print(i)
    res = requests.get('https://www.cheapshark.com/api/1.0/games?title='+i)
    data = res.text
    
    df2 = pd.read_json(data)
    #cheapstark = pd.concat([cheapstark, df2], ignore_index=True)
    
    count = count+ 1
    if count == 7:
        break


#print("hello")
print(cheapstark)

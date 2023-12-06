import pandas as pd
import numpy as np
import json
from pprint import pprint
from read_data import readCsv

import sys
import time
from datetime import datetime
from datetime import timedelta
import pycountry
from slugify import slugify
from igdb.wrapper import IGDBWrapper

requestCounter = 2395

def postRequest(endpoint: str, query: str) -> dict:
    wrapper = IGDBWrapper(client_id='0w5wnaqt03sq4l157jtmv6sz4pib2t',auth_token='31idnk8djqqn1cm1wrqmeuy8v2dfm8')
    byte_array = wrapper.api_request(
        endpoint=endpoint,
        query=query)
    byte_array_str = byte_array.decode().replace("'",'"')
    jsonData = dict()
    if len(byte_array_str) > 2:
        jsonData = json.loads(byte_array_str)[0]
    return jsonData

def getCompanyDataForGame(companiesList: list):
    global requestCounter
    company_df = pd.DataFrame({'company_id':pd.Series(dtype='int'),
                               'company_name': pd.Series(dtype='str'),
                               'company_url': pd.Series(dtype='str'),
                               'company_country': pd.Series(dtype='str'),
                               'company_startyear': pd.Series(dtype='int')})
    for involvedCompanyId in companiesList:
        companyOverviewJson = postRequest(endpoint='involved_companies',query='fields company; where id={};'.format(involvedCompanyId))
        requestCounter = requestCounter - 1
        if "company" in companyOverviewJson:
            companyJson = postRequest(endpoint='companies',query='fields country,name,url,start_date; where id={};'.format(companyOverviewJson['company']))
            requestCounter = requestCounter - 1
            countryName = np.nan
            startYear = np.nan
            
            if "country" in companyJson:
                countryCode = str(companyJson['country']) #cleaning country code 
                if len(countryCode) == 2:
                    countryCode = "0" + countryCode
                countryName = str(pycountry.countries.get(numeric=countryCode).name)
                
            if "start_date" in companyJson:    
                startDateTimestamp = companyJson['start_date']
                if startDateTimestamp < 0:
                    startYear = (datetime(1970, 1, 1) + timedelta(milliseconds=startDateTimestamp)).strftime('%Y')
                else:
                    startYear = datetime.fromtimestamp(startDateTimestamp).strftime('%Y')
            
            company_df = pd.concat([company_df,pd.DataFrame([[companyOverviewJson['company'],companyJson['name'],companyJson['url'],countryName,startYear]],columns=['company_id','company_name','company_url','company_country','company_startyear'])],axis=0)
    return company_df
    
def pullIgdbData():
    global requestCounter
    df = readCsv(path='../datasets/globalDF2.csv')
    fileCounter = 1
    logFileName = '../datasets/igdbLogs.txt'
    sys.stdout = open(logFileName,'w')
    companyData_df = pd.DataFrame(columns={'name': pd.Series(dtype='str'),
                                           'year': pd.Series(dtype='int'),
                                           'company_id': pd.Series(dtype='int'),
                                           'company_name': pd.Series(dtype='str'),
                                           'company_url': pd.Series(dtype='str'),
                                           'company_country': pd.Series(dtype='str'),
                                           'company_startyear': pd.Series(dtype='int')})
    for i in range(len(df)):
        name = df.loc[i,'name']
        year = df.loc[i,'year']
        print('-------------------------------\nLooking for companies for game: {}, released in {}'.format(name,year))
        jsonData = postRequest(endpoint='games',query='fields *; where slug="{}";'.format(slugify(name)))
        requestCounter = requestCounter - 1
        if 'involved_companies' in jsonData:
            if (requestCounter - 2*len(jsonData['involved_companies'])) <= 0:
                print('Start querying from game name: {}'.format(name))
                print('Start querying from index i = {}'.format(i))
                print('Start executing queries from time: {} + 5 seconds'.format(datetime.now() + timedelta(hours=1)))
                companyData_df.to_csv(path_or_buf='../datasets/subsetCompanyData_{}.csv'.format(fileCounter),sep=',',index=False,encoding='utf-8')
                fileCounter = fileCounter + 1
                time.sleep(600)
                requestCounter = 2395
            print('Companies found for => {}'.format(name))
            print('Company IDs are: {}'.format(jsonData['involved_companies']))
            game_df = getCompanyDataForGame(jsonData['involved_companies'])
            game_df.insert(0,"name",[name]*len(jsonData['involved_companies']))
            game_df.insert(1,"year",[year]*len(jsonData['involved_companies']))
            companyData_df = pd.concat([companyData_df,game_df],axis=0)
            print('request counter is = {}'.format(requestCounter))

    companyData_df.to_csv(path_or_buf='../datasets/companyData.csv',sep=',',index=False,encoding='utf-8')
    return companyData_df

companyData_df = pullIgdbData()
print('------------------------------------------------------------------------')
print('Final Company Data is => ')
print(companyData_df)
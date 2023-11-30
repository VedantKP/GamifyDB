import pandas as pd
import numpy as np
from read_data import readCsv
import time
import json
import requests


def getCheapStarkAPI():
    df = readCsv(path='../datasets/globalDF.csv',schema=['name'])
    cheapstark =  pd.DataFrame({'name': pd.Series(dtype='str'),
                                'id':pd.Series(dtype='str')})

    for i in df['name']:
        #print(i)
        res = requests.get('https://www.cheapshark.com/api/1.0/games?title='+i+'&exact=1')
        data = json.loads(res.text)[0] #ensure "data" is a dict
        print('type of data => {}'.format(type(data)))
        if "id" in data:
            id = data['gameId']
            cheapstark.insert(len(cheapstark),'name',i)
            cheapstark.insert(len(cheapstark),'id',id)
        #print(data)
        time.sleep(3)
    cheapstark.to_csv(path_or_buf='../datasets/cheapstarkExtract.csv',sep=';',encoding='utf-8')
    return cheapstark

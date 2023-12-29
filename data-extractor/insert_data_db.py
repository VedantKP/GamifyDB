import pandas as pd
from read_data import readCsv
from sqlalchemy import create_engine 

gameDfSchema = ['name','rating','votes','year','url','plot','Genre','NA_Sales','EU_Sales','JP_Sales','Other_Sales','Global_Sales']
companyDfSchema = ['name','year','company_id','company_name','company_url','company_country','company_startyear']
connString = 'postgresql://postgres:<password>@localhost:5432/gamify_db'

def readDf():
    gameDf = readCsv(path='../datasets/globalDF3.csv',schema=gameDfSchema)
    gameDf.insert(0,'id',range(1,len(gameDf)+1))
    gameDf.columns = [col.lower() for col in gameDf.columns]
    gameDf['votes'] = gameDf['votes'].str.replace(',','').astype(int)
    
    companyDf = readCsv(path='../datasets/allCompanyData.csv',schema=companyDfSchema)
    companyDf = companyDf.rename(columns={'company_id':'id'})
    companyDf['company_startyear'] = pd.to_numeric(companyDf['company_startyear'], errors='coerce')
    print(companyDf.dtypes)
    return gameDf, companyDf

def insertIntoGame(df:pd.DataFrame, conn):
    tableName = 'game'
    try:
        numRows = df.to_sql(tableName,conn,if_exists='append',index=False)
        if numRows > 0:
            print('Data inserted into game table!')
            return True
    except Exception as exp:
        print('Exception occured!: Data already exists in table')
    return False

def insertIntoCompany(companyDf: pd.DataFrame, conn):
    tableName = 'company'
    onlyCompanyDf = companyDf[['id','company_name','company_url','company_country','company_startyear']]
    onlyCompanyDf = onlyCompanyDf.drop_duplicates('id')
    onlyCompanyDf = onlyCompanyDf.sort_values(by='id')
    try:
        numRows = onlyCompanyDf.to_sql(tableName,conn,if_exists='append',index=False)
        if numRows > 0:
            print('Data inserted into company table!')
            return True
    except Exception as exp:
        print('Exception occured!: Data already exists in table')
    return False

def insertIntoGameDevelopers(game_df: pd.DataFrame, company_df: pd.DataFrame, conn):
    tableName = 'game_developers'
    merged_df = company_df.merge(game_df[['name', 'id']], how='left', on='name')
    merged_df.rename(columns={'id_y': 'game_id'}, inplace=True)
    # print('\n\nAfter renaming column\n')
    # print(merged_df)
    company_df['game_id'] = merged_df['game_id']
    # print(gameDev_df.sort_values(by=['game_id','company_id']))
    
    # print(company_df)
    # company_df.to_csv('../datasets/mergedGamesAndCompany.csv',sep=',',index=False,encoding='utf-8')
    gameDev_df = company_df[['game_id','id']]
    gameDev_df = gameDev_df.rename(columns={'id':'company_id'})
    gameDev_df = gameDev_df.sort_values(by=['game_id','company_id'])
    # print('before dropping duplicates, shape: {}'.format(gameDev_df.shape))
    # duplicates = gameDev_df[gameDev_df.duplicated(subset=['game_id', 'company_id'], keep=False)]
    # print(duplicates)
    gameDev_df.drop_duplicates(subset=['game_id','company_id'],inplace=True)
    # print('after dropping duplicates, shape: {}'.format(gameDev_df.shape))
    try:
        numRows = gameDev_df.to_sql(tableName,conn,if_exists='append',index=False)
        if numRows > 0:
            print('Data inserted into game developers table!')
            return True
    except Exception as exp:
        print('Exception occured!: Data already exists in table')
    
    return False

def createConnection():
    conn = create_engine(connString) 
    return conn

gameDf, companyDf = readDf()
conn = createConnection()
insertIntoGame(gameDf,conn=conn)
insertIntoCompany(companyDf,conn=conn)
insertIntoGameDevelopers(gameDf,companyDf,conn=conn)
conn.dispose()
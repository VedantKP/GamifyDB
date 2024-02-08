import pandas as pd
from read_data import readCsv
from sqlalchemy import create_engine 

gameDfSchema = ['name','rating','votes','year','url','plot','Genre','NA_Sales','EU_Sales','JP_Sales','Other_Sales','Global_Sales']
companyDfSchema = ['name','year','company_id','company_name','company_url','company_country','company_startyear']
connString = 'postgresql://postgres:<password>@localhost:5432/gamify_db'

'''
Function reads and returns both the dataframes needed to create the three tables as per the global schema
'''
def readDf():
    gameDf = readCsv(path='../datasets/globalDF.csv',schema=gameDfSchema)
    gameDf.insert(0,'id',range(1,len(gameDf)+1)) # creates an ID field
    gameDf.columns = [col.lower() for col in gameDf.columns] # sets name of columns to the name in the database
    gameDf['votes'] = gameDf['votes'].str.replace(',','').astype(int)
    
    companyDf = readCsv(path='../datasets/allCompanyData.csv',schema=companyDfSchema)
    companyDf = companyDf.rename(columns={'company_id':'id'}) # renames column to the name in the database
    companyDf['company_startyear'] = pd.to_numeric(companyDf['company_startyear'], errors='coerce')
    print(companyDf.dtypes)
    return gameDf, companyDf


'''
Function gets game data as an input dataframe and it stores the data into the 'game' table in the database
'''
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


'''
Function gets the developer data as an input dataframe and it stores the data into the 'company' table in the database
'''
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


'''
Function creates a bridge table between the game and company tables to ensure data is stored in a normalized form in the database.
'''
def insertIntoGameDevelopers(game_df: pd.DataFrame, company_df: pd.DataFrame, conn):
    tableName = 'game_developers'
    merged_df = company_df.merge(game_df[['name', 'id']], how='left', on='name')
    merged_df.rename(columns={'id_y': 'game_id'}, inplace=True)
    company_df['game_id'] = merged_df['game_id']
    gameDev_df = company_df[['game_id','id']]
    gameDev_df = gameDev_df.rename(columns={'id':'company_id'})
    gameDev_df = gameDev_df.sort_values(by=['game_id','company_id'])
    gameDev_df.drop_duplicates(subset=['game_id','company_id'],inplace=True)
    try:
        numRows = gameDev_df.to_sql(tableName,conn,if_exists='append',index=False)
        if numRows > 0:
            print('Data inserted into game developers table!')
            return True
    except Exception as exp:
        print('Exception occured!: Data already exists in table')
    
    return False


'''
Function creates and returns connection to the database
'''
def createConnection():
    conn = create_engine(connString) 
    return conn


gameDf, companyDf = readDf() # read both the dataframes
conn = createConnection() # create connection to the database
insertIntoGame(gameDf,conn=conn)
insertIntoCompany(companyDf,conn=conn)
insertIntoGameDevelopers(gameDf,companyDf,conn=conn)
conn.dispose()

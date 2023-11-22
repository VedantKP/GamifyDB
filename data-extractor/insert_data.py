import psycopg2

dbDetails = {
    'database': 'postgres',
    'user': 'postgres',
    'host': 'localhost',
    'password': 'TomHanks24$',
    'port': 5432
}

def createDb(dbName='gamify_db'):
    conn = psycopg2.connect(database = 'postgres',
                            user = 'postgres',
                            host = 'localhost',
                            password = 'TomHanks24$',
                            port = 5432)
    conn.autocommit = True
    cursor = conn.cursor()
    sql = 'create database {}'.format(dbName)
    try:
        cursor.execute(sql)
        print('{} database created!'.format(dbName))
    except Exception as exp:
        print('cannot create database: {}'.format(exp))
    cursor.close()

def createGlobalSchema(dbName=None,tableQueries=[None]):
    conn = psycopg2.connect(database = dbDetails['database'],
                            user = dbDetails['user'],
                            host = dbDetails['host'],
                            password = dbDetails['password'],
                            port = dbDetails['port'])
    conn.autocommit = True
    if dbName:
        createDb(conn,dbName=dbName)
    else: 
        print('No database name given!')
        return
    
    if tableQueries[0]:
        createTables(conn,dbname=dbName,queries=tableQueries)
    else:
        print('No table queries given to create tables!')
        return
    
    print('Global Schema created!')
    conn.close()
    return True
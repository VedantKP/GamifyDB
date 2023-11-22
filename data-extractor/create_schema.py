import psycopg2

dbDetails = {
    'user': 'postgres',
    'host': 'localhost',
    'password': '<password>',
    'port': 5432
}

tableQueries = [
    '''create table imdb (
        id serial primary key,
        name varchar(100) not null,
        url varchar(60) not null,
        year int not null,
        rating decimal not null,
        votes int not null,
        plot varchar(255) not null
    );'''
]

'''
Function creates multiple tables based on input queries
'''
def createTables(conn,queries=[None]):
    cursor = conn.cursor()
    for query in queries:
        print('executing query: {}'.format(query))
        try:
            cursor.execute(query)
        except Exception as exp:
            print('Cannot execute query: {} ... aborting further table creation'.format(exp))
            cursor.close()
            return
    print('All tables created!')
    cursor.close()

'''
Function creates a database and closes the connection
'''
def createDb(conn,dbName:str='gamify_db'):
    cursor = conn.cursor()
    sql = 'create database {}'.format(dbName)
    try:
        cursor.execute(sql)
        print('{} database created!'.format(dbName))
    except Exception as exp:
        print('cannot create database: {}'.format(exp))
    cursor.close()
    conn.close()

'''
Function connects to a particular database and returns the connection object
'''
def connectToDb(dbName:str=None):
    if dbName:
        conn = psycopg2.connect(database = dbName,
                                user = dbDetails['user'],
                                host = dbDetails['host'],
                                password = dbDetails['password'],
                                port = dbDetails['port'])
        return conn
    else:
        print('Cannot establish connection to a database as no database specified!')


'''
Function creates the global schema based on the database name and the table queries provided to it.
'''
def createGlobalSchema(dbName:str='gamify_db',tableQueries=[None]):
    conn = connectToDb(dbName='postgres')
    conn.autocommit = True
    createDb(conn,dbName=dbName)
    conn = connectToDb(dbName=dbName)
    conn.autocommit = True

    if tableQueries[0]:
        createTables(conn,queries=tableQueries)
    else:
        print('No table queries given to create tables!')
        conn.close()
        return
    
    conn.close()
    print('Global Schema created/exists!')
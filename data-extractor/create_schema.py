import psycopg2

dbDetails = {
    'user': 'postgres',
    'host': 'localhost',
    'password': '123',
    'port': 5432
}

tableQueries = [
    '''create table game (
        id int primary key,
        name varchar(100) not null,
        year int not null,
        rating decimal not null,
        votes int not null,
        url varchar(60) not null,
        plot varchar(355),
        genre varchar(15),
        NA_Sales decimal not null,
        EU_Sales decimal not null,
        JP_Sales decimal not null,
        Other_Sales decimal not null,
        Global_Sales decimal not null
    );''',
    '''create table company (
        id int primary key,
        company_name varchar(100),
        company_url varchar(131),
        company_country varchar(56),
        company_startyear int
    );''',
    '''create table game_developers (
        game_id int,
        company_id int,
        primary key (game_id,company_id),
        constraint fk_game foreign key(game_id) references game(id),
        constraint fk_company foreign key(company_id) references company(id) 
    );''',
    '''
    comment on column game.NA_Sales is 'North America sales in USD Millions';
    ''',
    '''
    comment on column game.EU_Sales is 'EU sales in USD Millions';
    ''',
    '''
    comment on column game.JP_Sales is 'Japan sales in USD Millions';
    ''',
    '''
    comment on column game.Other_Sales is 'Rest of the world sales in USD Millions';
    ''',
    '''
    comment on column game.Global_Sales is 'Worldwide sales in USD Millions';
    '''
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

createGlobalSchema(dbName='gamify_db',tableQueries=tableQueries)

import psycopg2

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
    conn.close()
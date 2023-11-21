import psycopg2

def createDb(dbName='gamify_db'):
    conn = psycopg2.connect(database = 'postgres',
                            user = 'postgres',
                            host = 'localhost',
                            password = '<password>',
                            port = 5432)
    conn.autocommit = True
    cursor = conn.cursor()
    sql = 'create database {}'.format(dbName)
    cursor.execute(sql)
    print('{} database created!'.format(dbName))
    conn.close()
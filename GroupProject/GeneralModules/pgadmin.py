import psycopg2

def makeconnection(host, database, user, password):
    '''Deze functie maakt een connectie met een PGAdmin database'''
    connection = psycopg2.connect(
                    database=database,
                    user=user,
                    password=password)
    return connection


def makecursor(connection):
    '''Deze functie maakt een cursor die nodig is om queries uit te voeren'''
    return connection.cursor()


def closeconnection(connection, cursor):
    '''Deze functie sluit de connectie met de PGAdmin database'''
    cursor.close()
    connection.close()


def insertdata(cursor, query, values=None):
    '''Deze functie voert data in in de database'''
    if values:
        cursor.execute(query, values)
    else:
        cursor.execute(query)


def getdata(cursor, query, fetchone=True, values=None):
    '''Deze functie haalt data op uit de PGAdmin database'''
    if values:
        cursor.execute(query, values)
    else:
        cursor.execute(query)
    if fetchone:
        return cursor.fetchone()
    return cursor.fetchall()


def executequery(cursor, query):
    '''Deze functie voert nog overige queries uit die nog wat complexer zijn en niet binne de scope van
    voorgaande functies vallen'''
    cursor.execute(query)

import psycopg2
# deze module is tijdelijk i.v.m. dat de module in de andere directory niet altijd herkend wordt
# bij het runnen van een shell script

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


def insertdata(cursor, query, values):
    '''Deze functie voert data in in de database'''
    cursor.execute(query, values)


def getdata(cursor, query, values, fetchone=True):
    '''Deze functie haalt data op uit de PGAdmin database'''
    cursor.execute(query, values)
    if fetchone:
        return cursor.fetchone()
    return cursor.fetchall()
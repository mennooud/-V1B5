import PGAdmin as P
import MongoDB as M
from pymongo import MongoClient


def deletecolumn(cursor, table, columnname, normtablename=None):
    if normtablename:
        column = normtablename+columnname+'id'
    else:
        column = columnname
    cursor.execute(f'ALTER TABLE {table} DROP COLUMN {column}')
    cursor.execute(f'DROP TABLE {normtablename}')


def addcolumn(cursor, table, columnname, normtablename=None):
    if normtablename:
        column = normtablename+columnname+'id'
    else:
        column = columnname
    P.executequery(cursor, f'ALTER TABLE {table} ADD COLUMN {column}')


client = MongoClient()
db = client.huwebshop
allproducts = M.getitems(db.products)

connection= P.makeconnection('localhost', 'test', 'postgres', 'broodje123')
cursor = P.makecursor(connection)
addcolumn(cursor, allproducts, 'properties.doelgroep', None)
P.closeconnection(connection, cursor)
import PGAdmin as P
import MongoDB as M
from pymongo import MongoClient


def deletecolumn(cursor, table, columnname, normtablename=None):
    if normtablename:
        cursor.execute(f'ALTER TABLE {table} DROP COLUMN {normtablename+columnname+"id"}')
        cursor.execute(f'DROP TABLE {normtablename}')
    else:
        cursor.execute(f'ALTER TABLE {table} DROP COLUMN {columnname}')




def addcolumn(cursor, table, columnname, normtablename=None):
    if normtablename:
        P.executequery(cursor, f'CREATE TABLE {normtablename} ({columnname+"id"} SERIAL PRIMARY KEY , {columnname} VARCHAR)')
        P.executequery(cursor, f'ALTER TABLE {table} ADD COLUMN {normtablename+columnname+"id"} INT')
        P.executequery(cursor, f'ALTER TABLE {table} ADD FOREIGN KEY ({normtablename+columnname+"id"}) REFERENCES '
                               f'{normtablename}({columnname+"id"})')
    else:
        P.executequery(cursor, f'ALTER TABLE {table} ADD COLUMN {columnname} VARCHAR')



client = MongoClient()
db = client.huwebshop
allproducts = M.getitems(db.products)

connection= P.makeconnection('localhost', 'test', 'postgres', 'broodje123')
cursor = P.makecursor(connection)
# addcolumn(cursor, 'products', 'doelgroep')
deletecolumn(cursor, 'products', 'doelgroep')
connection.commit()
P.closeconnection(connection, cursor)
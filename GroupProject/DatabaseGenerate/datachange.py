import pgadmin as P
import mongodb as M
from pymongo import MongoClient


def delete_column(cursor, table, columnname, normtablename=None):
    '''Deze functie kan een kolom en de eventueel bijbehorende normaliseringstabel weghalen'''
    if normtablename:
        cursor.execute(f'ALTER TABLE {table} DROP COLUMN IF EXISTS {normtablename+columnname+"id"}')
        cursor.execute(f'DROP TABLE IF EXISTS {normtablename}')
    else:
        cursor.execute(f'ALTER TABLE {table} DROP COLUMN {columnname}')


def add_column(cursor, table, columnname, normtablename=None):
    '''Deze functie maakt een nieuwe kolom aan en kan hier ook een normaliseringstabel
    voor maken als hier een naam voor wordt meegegeven'''
    if normtablename:
        P.executequery(cursor, f'CREATE TABLE {normtablename} ({columnname+"id"} SERIAL PRIMARY KEY , '
                               f'{columnname} VARCHAR)')
        P.executequery(cursor, f'ALTER TABLE {table} ADD COLUMN {normtablename+columnname+"id"} INT')
        P.executequery(cursor, f'ALTER TABLE {table} ADD FOREIGN KEY ({normtablename+columnname+"id"}) '
                               f'REFERENCES {normtablename}({columnname+"id"})')
    else:
        P.executequery(cursor, f'ALTER TABLE {table} ADD COLUMN {columnname} VARCHAR')


def add_data_to_column(cursor, data, table, idcolumn, oldcolumnname, columnname, normtablename=None):
    '''Deze functie vult de meegegeven data in in de meegegeven kolom en eventueel de
    meegegeven normaliseringstabel'''
    column = oldcolumnname.split('.')
    for item in data:
        cont = False
        id = item['_id']
        # de kolom die men wil invoeren kan nested zijn
        for nested in column:
            try:
                item = item[nested]
            except (KeyError, TypeError):
                cont = True
                continue
        # als item none is of als de key niet bestaat hoeft de volgende query ook niet uitgevoerd te worden
        if cont or item is None:
            continue
        # deze condition checkt of er een normaliseringstabel is meegegeven, voert anders gewoon de waarde in
        # de meegegeven tabel
        if normtablename:
            query = f"INSERT INTO {normtablename}({columnname}) SELECT %s WHERE " \
                    f"NOT EXISTS (SELECT * FROM {normtablename} WHERE {columnname}=(%s))"
            P.insertdata(cursor, query, (item, item))
            inputvalue = P.getdata(cursor, f'SELECT {columnname+"id"} FROM {normtablename} '
                                           f"WHERE {columnname}=%s", values=(item,))
            P.insertdata(cursor, f"UPDATE {table} SET {normtablename+columnname+'id'}={inputvalue[0]} "
                                 f"WHERE {idcolumn}=%s", (id,))
        else:
            P.insertdata(cursor, f'UPDATE {table} SET {columnname}={item} WHERE {idcolumn}=%s', (id,))


# haalt de hele oorspronkelijke dataset voor de producten op
client = MongoClient()
db = client.huwebshop
allproducts = M.getitems(db.products)

# TODO: verander onderstaande gegevens naar gegevens die kloppen voor je lokale database
connection = P.makeconnection('localhost', 'temp', 'postgres', 'broodje123')
cursor = P.makecursor(connection)

delete_column(cursor, 'products', 'doelgroep', 'doelgroepen')

add_column(cursor, 'products', 'doelgroep', 'doelgroepen')
add_data_to_column(cursor, allproducts, 'products', 'productid', 'properties.doelgroep',
                   'doelgroep', 'doelgroepen')
connection.commit()

P.closeconnection(connection, cursor)

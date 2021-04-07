from pymongo import MongoClient
import psycopg2
import mongodb  # TODO: markeer 'GeneralModules' directory als 'Sources Root'
import pgadmin
import json


def create_database(connection, cursor):
    '''Deze functie maakt nieuwe tabellen aan in de database, zodat deze later gevuld kunnen worden met de
    overgezette data'''
    pgadmin.executequery(cursor,
                         "DROP TABLE IF EXISTS TopSold; "
                         "DROP TABLE IF EXISTS Orderedproducts; "
                         "DROP TABLE IF EXISTS Recommendedproducts; "
                         "DROP TABLE IF EXISTS Viewedproducts; "
                         "DROP TABLE IF EXISTS Products; "
                         "DROP TABLE IF EXISTS Brands; "
                         "DROP TABLE IF EXISTS Categories; "
                         "DROP TABLE IF EXISTS Genders; "
                         "DROP TABLE IF EXISTS Sessions; "
                         "DROP TABLE IF EXISTS Profiles; "
                         "DROP TABLE IF EXISTS Sub_categories; "
                         "DROP TABLE IF EXISTS Sub_sub_categories; "
                         "DROP TABLE IF EXISTS Doelgroepen; "
                         "CREATE TABLE Brands (brandid SERIAL, brand varchar(255), PRIMARY KEY (brandid)); "
                         "CREATE TABLE Categories (categoryid SERIAL, category varchar(255) NOT NULL, "
                         "PRIMARY KEY (categoryid)); "
                         "CREATE TABLE Genders (genderid SERIAL, gender varchar(255), PRIMARY KEY (genderid)); "
                         "CREATE TABLE Orderedproducts (orderedproductid SERIAL, Sessionssessionid "
                         "varchar(255) NOT NULL, Productsproductid varchar(255), PRIMARY KEY (orderedproductid)); "
                         "CREATE TABLE Products (productid varchar(255) NOT NULL, name varchar(255), "
                         "description varchar(1023), price int, herhaalaankopen BIT, recommendable BIT, "
                         "Brandsbrandid int, Categoriescategoryid int, Sub_sub_categoriessub_sub_categoryid int, "
                         "Sub_categoriessub_categoryid int, Gendersgenderid int, Doelgroependoelgroepid int, "
                         "PRIMARY KEY (productid)); "
                         "CREATE TABLE Profiles (profileid varchar(255) NOT NULL, PRIMARY KEY (profileid)); "
                         "CREATE TABLE Recommendedproducts (recommendedproductid SERIAL, "
                         "Profilesprofileid varchar(255) NOT NULL, Productsproductid varchar(255), "
                         "PRIMARY KEY (recommendedproductid)); "
                         "CREATE TABLE Sessions (sessionid varchar(255) NOT NULL, Profilesprofileid varchar(255), "
                         "sessionstart timestamp NOT NULL, sessionend timestamp NOT NULL, has_sale BIT NOT NULL, "
                         "PRIMARY KEY (sessionid)); "
                         "CREATE TABLE Sub_categories (sub_categoryid SERIAL, sub_category varchar(255) NOT NULL, "
                         "PRIMARY KEY (sub_categoryid)); "
                         "CREATE TABLE Sub_sub_categories (sub_sub_categoryid SERIAL, "
                         "sub_sub_category varchar(255) NOT NULL, PRIMARY KEY (sub_sub_categoryid)); "
                         "CREATE TABLE Doelgroepen (doelgroepid SERIAL, "
                         "doelgroep varchar(255) NOT NULL, PRIMARY KEY (doelgroepid)); "
                         "CREATE TABLE Viewedproducts (viewedproductid SERIAL, Profilesprofileid varchar(255) NOT NULL, "
                         "Productsproductid varchar(255), PRIMARY KEY (viewedproductid)); "
                         "ALTER TABLE Viewedproducts ADD CONSTRAINT FKViewedprod516306 FOREIGN KEY (Productsproductid) "
                         "REFERENCES Products (productid); "
                         "ALTER TABLE Viewedproducts ADD CONSTRAINT FKViewedprod836821 FOREIGN KEY (Profilesprofileid) "
                         "REFERENCES Profiles (profileid); "
                         "ALTER TABLE Recommendedproducts ADD CONSTRAINT FKRecommende637348 "
                         "FOREIGN KEY (Productsproductid) REFERENCES Products (productid); "
                         "ALTER TABLE Recommendedproducts ADD CONSTRAINT FKRecommende316833 "
                         "FOREIGN KEY (Profilesprofileid) REFERENCES Profiles (profileid); "
                         "ALTER TABLE Products ADD CONSTRAINT FKProducts505288 FOREIGN KEY (Gendersgenderid) "
                         "REFERENCES Genders (genderid); "
                         "ALTER TABLE Products ADD CONSTRAINT FKProducts973090 FOREIGN KEY "
                         "(Sub_categoriessub_categoryid) REFERENCES Sub_categories (sub_categoryid); "
                         "ALTER TABLE Products ADD CONSTRAINT FKProducts978174 FOREIGN KEY "
                         "(Sub_sub_categoriessub_sub_categoryid) REFERENCES Sub_sub_categories (sub_sub_categoryid); "
                         "ALTER TABLE Sessions ADD CONSTRAINT FKSessions493177 FOREIGN KEY (Profilesprofileid) "
                         "REFERENCES Profiles (profileid); "
                         "ALTER TABLE Orderedproducts ADD CONSTRAINT FKOrderedpro121984 FOREIGN KEY "
                         "(Productsproductid) REFERENCES Products (productid); "
                         "ALTER TABLE Orderedproducts ADD CONSTRAINT FKOrderedpro685696 FOREIGN KEY "
                         "(Sessionssessionid) REFERENCES Sessions (sessionid); "
                         "ALTER TABLE Products ADD CONSTRAINT FKProducts897994 FOREIGN KEY (Categoriescategoryid) "
                         "REFERENCES Categories (categoryid); "
                         "ALTER TABLE Products ADD CONSTRAINT FKProducts577926 FOREIGN KEY (Brandsbrandid) "
                         "REFERENCES Brands (brandid); "
                         "ALTER TABLE Products ADD CONSTRAINT FKProducts10000 FOREIGN KEY (Doelgroependoelgroepid) "
                         "REFERENCES Doelgroepen (doelgroepid); ")
    connection.commit()


def inputproducts(items, connection, cursor, newcolumns):
    '''Deze functie zet meegegeven data in de relationele database op basis van de
    meegegeven nieuwe kolommen'''
    for item in items:
        # deze dictionary houdt bij welke waardes in een rij in de 'Products' tabel gaat staan
        productdict = {}
        skip = False
        for key in item.keys():
            # vergelijkt alle keys met de keys in de meegegeven dictionary om alleen de gevraagde waardes
            # over te zetten
            if key in newcolumns.keys():
                # als 'Products' niet in de waarde zit van de meegegeven dictionary betekent dit dat
                # deze waarde in een andere tabel moet als als foreign key in de 'Products' tabel
                if 'Products' not in newcolumns[key]:
                    table = newcolumns[key].split('(')[0]
                    newcolumn = newcolumns[key].split('(')[1].replace(')', '')
                    returnedvalue = newcolumn + 'id'
                    selectquery = f'select {returnedvalue} from {table} where {newcolumn}=(%s)'
                    if key == 'properties':
                        try:
                            item[key] = item['properties']['doelgroep']
                        except KeyError:
                            item[key] = None
                    if item[key] is not None:
                        if isinstance(item[key], list):
                            item[key] = str(item[key])
                        pgadmin.insertdata(cursor, f'insert into {newcolumns[key]}'
                                                   f'select (%s) where not exists ({selectquery})',
                                           (item[key], item[key]))
                        column = pgadmin.getdata(cursor, selectquery, values=(item[key],))[0]
                    else:
                        column = None
                    productdict[newcolumns[key].replace('(', '').replace(')', '')+'id'] = column
                else:
                    if key == 'price':
                        # de kolom 'price' is de enige kolom van de kolommen die we gebruiken
                        # die als waarde een dictionary heeft
                        productdict[key] = item[key]['selling_price']
                    else:
                        if isinstance(item[key], bool):
                            # zet een boolean om naar een bit type voor de relationele database
                            value = f'B{int(item[key])}'
                        else:
                            value = item[key]
                        productdict[newcolumns[key].split('(')[1].replace(')', '')] = value
        if skip:
            continue
        columns, values = list(productdict.keys()), list(productdict.values())
        inputcolumns = ','.join(columns)
        inputvalues = ','.join(['%s']*len(values))
        insertquery = 'insert into Products({}) values ({})'.format(inputcolumns, inputvalues)
        pgadmin.insertdata(cursor, insertquery, values)
    connection.commit()


def inputsessions(sessions, buids, connection, cursor, newcolumns):
    progress = 0
    percentage = 0
    totallength = sessions.count()
    for session in sessions:
        progress, percentage = showprogress(totallength, progress, percentage)
        sessiondict = {}
        for key in session.keys():
            if key in newcolumns.keys():
                if key == 'buid':
                    if session[key] is not None:
                        try:
                            sessiondict[newcolumns[key]] = buids[str(session[key][0])]
                        except KeyError:
                            pass
                else:
                    if isinstance(session[key], bool):
                        value = f'B{int(session[key])}'
                    else:
                        value = session[key]
                    sessiondict[newcolumns[key]] = value
        columns, values = list(sessiondict.keys()), [item if isinstance(item, str) else str(item) for item in sessiondict.values()]
        inputcolumns = ','.join(columns)
        inputvalues = ','.join(['%s'] * len(values))
        insertquery = 'insert into Sessions({}) values ({})'.format(inputcolumns, inputvalues)
        cursor.execute(insertquery, values)
        if session['has_sale']:
            if 'order' in session.keys():
                if session['order'] is not None:
                    for order in session['order']['products']:
                        try:
                            orderquery = "insert into orderedproducts (sessionssessionid, productsproductid) " \
                                         "values ('{}', '{}')".format(session['_id'], order['id'])
                            # orderquery = f'insert into orderedproducts(sessionssessionid, productsproductid)' \
                            #              f'select where exists (select productid from products where productid='{session}')'
                            cursor.execute(orderquery)
                        except psycopg2.errors.ForeignKeyViolation:
                            cursor.execute('ROLLBACK')
    connection.commit()


def inputprofiles(profiles, connection, cursor, newcolumns):
    buids = {}
    progress = 0
    percentage = 0
    totallength = profiles.count()
    for profile in profiles:
        progress, percentage = showprogress(totallength, progress, percentage)
        profiledict = {}
        for key in profile.keys():
            if key in newcolumns.keys():
                if 'Profiles' in newcolumns[key]:
                    value = str(profile[key])
                    profiledict[newcolumns[key]] = value
                    insertquery = "insert into Profiles values (%s)"
                    cursor.execute(insertquery, (str(profile['_id']),))
                elif key == 'previously_recommended':
                    for product in profile[key]:
                        recommendquery = "insert into recommendedproducts (profilesprofileid, productsproductid) " \
                                         "VALUES ('{}', (select productid from products " \
                                         "where productid='{}'))".format(profile['_id'], product)
                        cursor.execute(recommendquery)
                else:
                    for product in profile[key]['viewed_before']:
                        recommendedquery = "insert into viewedproducts (profilesprofileid, productsproductid) " \
                                           "VALUES ('{}', (select productid from products " \
                                           "where productid='{}'))".format(profile['_id'], product)
                        cursor.execute(recommendedquery)
            else:
                if key == 'buids':
                    for buid in profile[key]:
                        buids[buid] = str(profile['_id'])
    connection.commit()
    return buids


def showprogress(totallength, progress, percentage):
    if progress/totallength*100 >= percentage:
        print(str(percentage)+' %')
        percentage += 1
    return progress+1, percentage


oldtonewproducts = {'_id': 'Products(productid)', 'brand': 'Brands(brand)', 'category': 'Categories(category)',
                    'description': 'Products(description)', 'herhaalaankopen': 'Products(herhaalaankopen)',
                    'gender': 'Genders(gender)', 'recommendable': 'Products(recommendable)',
                    'name': 'Products(name)', 'price': 'Products(price)',
                    'sub_category': 'Sub_categories(sub_category)',
                    'sub_sub_category': 'Sub_sub_categories(sub_sub_category)',
                    'properties': 'Doelgroepen(doelgroep)'}

oldtonewsessions = {'_id': 'sessionid', 'buid': 'profilesprofileid', 'session_start': 'sessionstart',
                    'session_end': 'sessionend', 'has_sale': 'has_sale'}

oldtonewprofiles = {'_id': 'Profiles(profileid)', 'previously_recommended': 'Recommendedproducts(recommended)',
                    'recommendations': 'Viewedproducts(viewed_before)'}

client = MongoClient()
db = client.huwebshop
collection = db.products
items = mongodb.getitems(collection)
sessioninfo = db.sessions
sessions = mongodb.getitems(sessioninfo)
profileinfo = db.profiles
profiles = mongodb.getitems(profileinfo)


# TODO: verander onderstaande gegevens zodat ze kloppen voor je lokale database
connection = pgadmin.makeconnection('localhost', 'testjuh', 'postgres', 'broodje123')
cursor = pgadmin.makecursor(connection)
print('Creating database')
create_database(connection, cursor)
print('Inputting products')
inputproducts(items, connection, cursor, oldtonewproducts)
print('Inputting profiles')
# buids = inputprofiles(profiles, connection, cursor, oldtonewprofiles)

# deze onderstaande code kan gebruikt worden als het niet lukt om de gehele tabel in één keer in te vullen
# de buids worden dan opgeslagen in een json file zodat het mogelijk is de tabel in stappen in te vullen

# file = 'buids.json'
# with open (file, 'r') as json_file:
#     buids2 = json.load(json_file)
# newbuids = dict(buids, **buids2)
# with open(file, 'w') as jsonfile:
#     json.dump(newbuids, jsonfile, indent=4)

print('Inputting sessions')
# inputsessions(sessions, buids, connection, cursor, oldtonewsessions)
pgadmin.closeconnection(connection, cursor)

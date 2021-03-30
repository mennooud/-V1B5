import PGAdmin as P


def maketable(cursor):
    P.executequery(cursor, f"DROP TABLE IF EXISTS profileproperties")
    P.executequery(cursor, f"CREATE TABLE profileproperties(profilesprofileid VARCHAR, doelgroep VARCHAR,"
                           f"bestcategory VARCHAR, bestsubcategory VARCHAR, bestbrand VARCHAR, herhaalpreference BIT,"
                           f"pricepreference VARCHAR, CONSTRAINT checkprice CHECK (pricepreference IN "
                           f"('LOW', 'MIDDLE', 'HIGH')), FOREIGN KEY (profilesprofileid) REFERENCES profiles(profileid))")


def filltable(cursor):
    pass



connection = P.makeconnection('localhost', 'test', 'postgres', 'broodje123')
cursor = P.makecursor(connection)
maketable(cursor)
connection.commit()
P.closeconnection(connection, cursor)
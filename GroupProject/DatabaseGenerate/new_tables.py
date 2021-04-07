import pgadmin


def top_sold(connection, cursor):
    '''Deze functie maakt een nieuwe tabel aan en zet daar de producten in op volgorde van
    waarin ze het meest verkocht zijn'''
    pgadmin.executequery(cursor, "DROP TABLE IF EXISTS TopSold; "
                                 "CREATE TABLE TopSold(id SERIAL, productid varchar(255) NOT NULL, "
                                 "freq bigint NOT NULL, PRIMARY KEY (id), "
                                 "FOREIGN KEY (productid) REFERENCES products(productid)); "
                                 "INSERT INTO TopSold (productid, freq) "
                                 "SELECT productsproductid, count(productsproductid) "
                                 "FROM orderedproducts "
                                 "GROUP by productsproductid "
                                 "ORDER BY count DESC")
    connection.commit()


def top_viewed(connection, cursor):
    '''Deze functie maakt een nieuwe tabel aan en zet daar de producten in op volgorde van
    waarin ze het meest bekeken zijn'''
    pgadmin.executequery(cursor, "DROP TABLE IF EXISTS TopViewed; "
                                 "CREATE TABLE TopViewed(id SERIAL, productid varchar(255) NOT NULL, "
                                 "freq bigint NOT NULL, PRIMARY KEY (id), "
                                 "FOREIGN KEY (productid) REFERENCES products (productid)); "
                                 "INSERT INTO TopViewed (productid, freq) "
                                 "SELECT productsproductid, count(productsproductid) "
                                 "FROM viewedproducts "
                                 "WHERE productsproductid IS NOT NULL "
                                 "GROUP by productsproductid "
                                 "ORDER BY count DESC")
    connection.commit()


connection = pgadmin.makeconnection('localhost', 'testtest', 'postgres', 'broodje123')
cursor = pgadmin.makecursor(connection)
print('Making table for most sold products...')
top_sold(connection, cursor)
print('Making table for most viewed products...')
top_viewed(connection, cursor)

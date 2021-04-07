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


def popular_products(connection, cursor):
    '''Deze functie maakt een nieuwe tabel aan en zet daar de producten in op volgorde van
    populariteit. Dit is een combinatie van hoevaak de producten bekeken en gekocht zijn.'''
    pgadmin.executequery(cursor, "DROP TABLE IF EXISTS popular; "
                                 "CREATE TABLE popular(id SERIAL, productid varchar(255), pop int, "
                                 "PRIMARY KEY (id), FOREIGN KEY (productid) REFERENCES products(productid)); "
                                 "INSERT INTO popular (productid, pop) "
                                 "SELECT topsold.productid, (topsold.id + topviewed.id) as pop "
                                 "FROM topsold, topviewed "
                                 "WHERE topsold.productid = topviewed.productid "
                                 "ORDER BY pop")
    connection.commit()


def product_combinations(connection, cursor):
    '''Deze functie maakt een nieuwe tabel aan en zet daar de mogelijke combinaties aan producteigenschappen
    in.'''
    pgadmin.executequery(cursor, "DROP TABLE IF EXISTS prodCombinations; "
                                 "CREATE TABLE prodCombinations (combid SERIAL, doelgroep int, "
                                 "category int, sub_category int, "
                                 "price varchar(255), brand int, herhaalaankopen BIT, "
                                 "PRIMARY KEY (combid), "
                                 "FOREIGN KEY (doelgroep) REFERENCES doelgroepen(doelgroepid), "
                                 "FOREIGN KEY (category) REFERENCES categories(categoryid), "
                                 "FOREIGN KEY (sub_category) REFERENCES sub_categories(sub_categoryid), "
                                 "FOREIGN KEY (brand) REFERENCES brands(brandid)); "
                                 "INSERT INTO prodCombinations (doelgroep, category, sub_category, price, "
                                 "brand, herhaalaankopen) "
                                 "SELECT distinct doelgroependoelgroepid, category, sub_categoriessub_categoryid, "
                                 "pr AS price, brandsbrandid, herhaalaankopen "
                                 "FROM (SELECT distinct categoriescategoryid AS category, "
                                 "sub_categoriessub_categoryid, doelgroependoelgroepid, "
                                 "brandsbrandid, herhaalaankopen, price, CASE WHEN price < 250 THEN  'low' "
                                 "WHEN price >= 250 AND price < 600 THEN 'mid' ELSE 'high' END AS pr "
                                 "FROM products) AS combi; DROP TABLE IF EXISTS prodcomb; "
                                 "CREATE TABLE prodcomb (productid varchar(255), combid int, "
                                 "FOREIGN KEY (productid) REFERENCES products(productid), "
                                 "FOREIGN KEY (combid) REFERENCES prodcombinations(combid)); "
                                 "INSERT INTO prodcomb (productid, combid) "
                                 "SELECT pr, comb FROM "
                                 "(SELECT prodcombinations, products.productid, products.price, "
                                 "products.herhaalaankopen, products.brandsbrandid, products.categoriescategoryid, "
                                 "products.sub_categoriessub_categoryid, products.doelgroependoelgroepid, "
                                 "CASE WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid "
                                 "OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL) "
                                 "AND (prodcombinations.category = products.categoriescategoryid "
                                 "OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL) "
                                 "AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid "
                                 "OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid "
                                 "IS NULL) AND (prodcombinations.brand = products.brandsbrandid "
                                 "OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL)	"
                                 "AND (prodcombinations.herhaalaankopen = products.herhaalaankopen "
                                 "OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL) "
                                 "AND prodcombinations.price = 'low' AND products.price < 250 THEN products.productid "
                                 "WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid "
                                 "OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL) "
                                 "AND (prodcombinations.category = products.categoriescategoryid "
                                 "OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL) "
                                 "AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid "
                                 "OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid "
                                 "IS NULL) AND (prodcombinations.brand = products.brandsbrandid "
                                 "OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL)	"
                                 "AND (prodcombinations.herhaalaankopen = products.herhaalaankopen "
                                 "OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL)	"
                                 "AND prodcombinations.price = 'mid' AND products.price >= 250 AND products.price < 600 "
                                 "THEN products.productid WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid "
                                 "OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL) "
                                 "AND (prodcombinations.category = products.categoriescategoryid "
                                 "OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL) "
                                 "AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid "
                                 "OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid "
                                 "IS NULL) AND (prodcombinations.brand = products.brandsbrandid "
                                 "OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL) "
                                 "AND (prodcombinations.herhaalaankopen = products.herhaalaankopen "
                                 "OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL) "
                                 "AND prodcombinations.price = 'high' "
                                 "AND products.price >= 600 THEN products.productid END AS pr, CASE "
                                 "WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid "
                                 "OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL) "
                                 "AND (prodcombinations.category = products.categoriescategoryid "
                                 "OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL) "
                                 "AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid "
                                 "OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid "
                                 "IS NULL) AND (prodcombinations.brand = products.brandsbrandid "
                                 "OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL) "
                                 "AND (prodcombinations.herhaalaankopen = products.herhaalaankopen "
                                 "OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL) "
                                 "AND prodcombinations.price = 'low' AND products.price < 250 THEN prodcombinations.combid "
                                 "WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid "
                                 "OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL) "
                                 "AND (prodcombinations.category = products.categoriescategoryid "
                                 "OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL) "
                                 "AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid "
                                 "OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid "
                                 "IS NULL) AND (prodcombinations.brand = products.brandsbrandid "
                                 "OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL) "
                                 "AND (prodcombinations.herhaalaankopen = products.herhaalaankopen "
                                 "OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL)	"
                                 "AND prodcombinations.price = 'mid' AND products.price >= 250 "
                                 "AND products.price < 600 THEN prodcombinations.combid "
                                 "WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid "
                                 "OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL) "
                                 "AND (prodcombinations.category = products.categoriescategoryid "
                                 "OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL) "
                                 "AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid "
                                 "OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid "
                                 "IS NULL) AND (prodcombinations.brand = products.brandsbrandid "
                                 "OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL) "
                                 "AND (prodcombinations.herhaalaankopen = products.herhaalaankopen "
                                 "OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL) "
                                 "AND prodcombinations.price = 'high' AND products.price >= 600 THEN prodcombinations.combid "
                                 "END AS comb FROM prodcombinations, products) as prod "
                                 "WHERE pr IS NOT NULL AND comb IS NOT NULL ORDER BY comb")
    connection.commit()


connection = pgadmin.makeconnection('localhost', 'testtest', 'postgres', 'broodje123')
cursor = pgadmin.makecursor(connection)
print('Making table for most sold products...')
top_sold(connection, cursor)
print('Making table for most viewed products...')
top_viewed(connection, cursor)
print('Making table for most popular products...')
popular_products(connection, cursor)
print('Making table for all product combinations...')
product_combinations(connection, cursor)

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


def profile_properties(connection, cursor):
    '''Deze functie maakt een tabel waarin het koopgedrag van de profielen die al producten gekocht
    hebben beschreven aan de hand van de eigenschappen van de gekochte producten.'''
    pgadmin.executequery(cursor, "DROP TABLE IF EXISTS profileproperties; "
                                 "CREATE TABLE profileproperties(profilesprofileid VARCHAR, doelgroep VARCHAR,"
                                 "bestcategory VARCHAR, bestsubcategory VARCHAR, bestbrand VARCHAR, "
                                 "herhaalpreference BIT,"
                                 "pricepreference VARCHAR, CONSTRAINT checkprice CHECK (pricepreference IN "
                                 "('LOW', 'MIDDLE', 'HIGH')), FOREIGN KEY (profilesprofileid) "
                                 "REFERENCES profiles(profileid)); ")
    pgadmin.executequery(cursor,
                         "WITH subquery AS(select profileid, doelgroep, category, sub_category, brand, herhaalaankopen,"
                         "price from profiles right join sessions on profileid=profilesprofileid "
                         "right join orderedproducts on sessionid=sessionssessionid "
                         "left join products on productsproductid=productid "
                         "left join categories on categoriescategoryid=categoryid "
                         "left join doelgroepen on doelgroependoelgroepid=doelgroepid "
                         "left join sub_categories on sub_categoriessub_categoryid=sub_categoryid "
                         "left join brands on brandsbrandid=brandid "
                         "where not profileid is null), "
                         "subdoelgroep1 AS(select profileid, doelgroep, count(doelgroep) as frequency from subquery "
                         "group by profileid, doelgroep), "
                         "subdoelgroep2 AS(select profileid, max(frequency) as mostfrequent from subdoelgroep1 "
                         "group by profileid), "
                         "subdoelgroep AS(select subdoelgroep2.profileid, doelgroep from subdoelgroep2 "
                         "left join subdoelgroep1 on subdoelgroep2.mostfrequent=subdoelgroep1.frequency and "
                         "subdoelgroep2.profileid=subdoelgroep1.profileid), "
                         "subcategory1 AS(select profileid, category, count(category) as frequency from subquery "
                         "group by profileid, category), "
                         "subcategory2 AS(select profileid, max(frequency) as mostfrequent from subcategory1 "
                         "group by profileid), "
                         "subcategory AS(select subcategory2.profileid, category from subcategory2 "
                         "left join subcategory1 on subcategory2.mostfrequent=subcategory1.frequency and "
                         "subcategory2.profileid=subcategory1.profileid), "
                         "subsubcategory1 AS(select profileid, sub_category, count(sub_category) as frequency from subquery "
                         "group by profileid, sub_category), "
                         "subsubcategory2 AS(select profileid, max(frequency) as mostfrequent from subsubcategory1 "
                         "group by profileid), "
                         "subsubcategory AS(select subsubcategory2.profileid, sub_category from subsubcategory2 "
                         "left join subsubcategory1 on subsubcategory2.mostfrequent=subsubcategory1.frequency and "
                         "subsubcategory2.profileid=subsubcategory1.profileid), "
                         "subbrand1 AS(select profileid, brand, count(brand) as frequency from subquery "
                         "group by profileid, brand), "
                         "subbrand2 AS(select distinct(profileid), max(frequency) as mostfrequent from subbrand1 "
                         "group by profileid), "
                         "subbrand AS(select subbrand2.profileid, brand from subbrand2 "
                         "left join subbrand1 on subbrand2.mostfrequent=subbrand1.frequency and "
                         "subbrand2.profileid=subbrand1.profileid), "
                         "subherhaal1 AS(select profileid, herhaalaankopen, count(herhaalaankopen) as frequency from subquery "
                         "group by profileid, herhaalaankopen), "
                         "subherhaal2 AS(select distinct(profileid), max(frequency) as mostfrequent from subherhaal1 "
                         "group by profileid), "
                         "subherhaal AS(select subherhaal2.profileid, herhaalaankopen from subherhaal2 "
                         "left join subherhaal1 on subherhaal2.mostfrequent=subherhaal1.frequency and "
                         "subherhaal2.profileid=subherhaal1.profileid), "
                         "subprice1 AS(select profileid, round(avg(price), 0) as averageprice from subquery "
                         "group by profileid), "
                         "subprice AS(select profileid, averageprice, "
                         "case when averageprice > 0 and averageprice<250 then 'LOW' "
                         "when averageprice>=250 and averageprice<=600 then 'MIDDLE' "
                         "when averageprice>600 then 'HIGH' "
                         "end as pricecategory "
                         "from subprice1)"
                         "insert into profileproperties "
                         "(SELECT subdoelgroep.profileid, doelgroep, category, sub_category, brand, "
                         "herhaalaankopen, pricecategory "
                         "from subdoelgroep left join subcategory "
                         "on subdoelgroep.profileid=subcategory.profileid "
                         "left join subsubcategory on subdoelgroep.profileid=subsubcategory.profileid "
                         "left join subbrand on subdoelgroep.profileid=subbrand.profileid "
                         "left join subherhaal on subdoelgroep.profileid=subherhaal.profileid "
                         "left join subprice on subdoelgroep.profileid=subprice.profileid)")
    connection.commit()


def ordered_profiles(connection, cursor):
    '''Deze functie maakt een tabel waarin de gekochte producten gekoppeld zijn aan de profielen, zodat
    deze niet meer middels meerdere join statements aan elkaar hoeven worden geplakt.'''
    pgadmin.executequery(cursor,"drop table if exists orderedprofiles; "
                                "create table orderedprofiles( profilesprofileid varchar, "
                                "orderedproductid varchar, FOREIGN KEY (profilesprofileid) references profiles(profileid), "
                                "FOREIGN KEY (orderedproductid) references products(productid));")
    pgadmin.executequery(cursor, "insert into orderedprofiles (select profileid, productsproductid from orderedproducts "
                                 "left join sessions on sessionssessionid=sessionid "
                                 "left join profiles on profilesprofileid=profileid "
                                 "where not profileid is null)")
    connection.commit()

connection = pgadmin.makeconnection('localhost', 'deechte', 'postgres', 'broodje123')
cursor = pgadmin.makecursor(connection)
print('Making table for most sold products...')
top_sold(connection, cursor)
print('Making table for most viewed products...')
top_viewed(connection, cursor)
print('Making table for most popular products...')
popular_products(connection, cursor)
print('Making table for all product combinations...')
product_combinations(connection, cursor)
print('Making table for all profile properties...')
profile_properties(connection, cursor)
print('Making table connecting ordered products and profiles...')
ordered_profiles(connection, cursor)
print('All done :)')

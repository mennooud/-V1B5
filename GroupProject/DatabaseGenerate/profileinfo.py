import PGAdmin as P


def maketable(cursor):
    P.executequery(cursor, f"DROP TABLE IF EXISTS profileproperties")
    P.executequery(cursor, f"CREATE TABLE profileproperties(profilesprofileid VARCHAR, doelgroep VARCHAR,"
                           f"bestcategory VARCHAR, bestsubcategory VARCHAR, bestbrand VARCHAR, herhaalpreference BIT,"
                           f"pricepreference VARCHAR, CONSTRAINT checkprice CHECK (pricepreference IN "
                           f"('LOW', 'MIDDLE', 'HIGH')), FOREIGN KEY (profilesprofileid) REFERENCES profiles(profileid))")


def filltable(cursor):
    P.executequery(cursor, f"WITH subquery AS(select profileid, doelgroep, category, sub_category, brand, herhaalaankopen,"
                           f"price from profiles right join sessions on profileid=profilesprofileid "
                           f"right join orderedproducts on sessionid=sessionssessionid "
                           f"left join products on productsproductid=productid "
                           f"left join categories on categoriescategoryid=categoryid "
                           f"left join doelgroepen on doelgroependoelgroepid=doelgroepid "
                           f"left join sub_categories on sub_categoriessub_categoryid=sub_categoryid "
                           f"left join brands on brandsbrandid=brandid "
                           f"where not profileid is null), "
                           f"subdoelgroep1 AS(select profileid, doelgroep, count(doelgroep) as frequency from subquery "
                           f"group by profileid, doelgroep), "
                           f"subdoelgroep2 AS(select profileid, max(frequency) as mostfrequent from subdoelgroep1 "
                           f"group by profileid), "
                           f"subdoelgroep AS(select subdoelgroep2.profileid, doelgroep from subdoelgroep2 "
                           f"left join subdoelgroep1 on subdoelgroep2.mostfrequent=subdoelgroep1.frequency and "
                           f"subdoelgroep2.profileid=subdoelgroep1.profileid), "
                           f"subcategory1 AS(select profileid, category, count(category) as frequency from subquery "
                           f"group by profileid, category), "
                           f"subcategory2 AS(select profileid, max(frequency) as mostfrequent from subcategory1 "
                           f"group by profileid), "
                           f"subcategory AS(select subcategory2.profileid, category from subcategory2 "
                           f"left join subcategory1 on subcategory2.mostfrequent=subcategory1.frequency and "
                           f"subcategory2.profileid=subcategory1.profileid), "
                           f"subsubcategory1 AS(select profileid, sub_category, count(sub_category) as frequency from subquery "
                           f"group by profileid, sub_category), "
                           f"subsubcategory2 AS(select profileid, max(frequency) as mostfrequent from subsubcategory1 "
                           f"group by profileid), "
                           f"subsubcategory AS(select subsubcategory2.profileid, sub_category from subsubcategory2 "
                           f"left join subsubcategory1 on subsubcategory2.mostfrequent=subsubcategory1.frequency and "
                           f"subsubcategory2.profileid=subsubcategory1.profileid), "
                           f"subbrand1 AS(select profileid, brand, count(brand) as frequency from subquery "
                           f"group by profileid, brand), "
                           f"subbrand2 AS(select distinct(profileid), max(frequency) as mostfrequent from subbrand1 "
                           f"group by profileid), "
                           f"subbrand AS(select subbrand2.profileid, brand from subbrand2 "
                           f"left join subbrand1 on subbrand2.mostfrequent=subbrand1.frequency and "
                           f"subbrand2.profileid=subbrand1.profileid), "
                           f"subherhaal1 AS(select profileid, herhaalaankopen, count(herhaalaankopen) as frequency from subquery "
                           f"group by profileid, herhaalaankopen), "
                           f"subherhaal2 AS(select distinct(profileid), max(frequency) as mostfrequent from subherhaal1 "
                           f"group by profileid), "
                           f"subherhaal AS(select subherhaal2.profileid, herhaalaankopen from subherhaal2 "
                           f"left join subherhaal1 on subherhaal2.mostfrequent=subherhaal1.frequency and "
                           f"subherhaal2.profileid=subherhaal1.profileid), "
                           f"subprice1 AS(select profileid, round(avg(price), 0) as averageprice from subquery "
                           f"group by profileid), "
                           f"subprice AS(select profileid, averageprice, "
                           f"case when averageprice > 0 and averageprice<250 then 'LOW' "
                           f"when averageprice>=250 and averageprice<=600 then 'MIDDLE' "
                           f"when averageprice>600 then 'HIGH' "
                           f"end as pricecategory "
                           f"from subprice1)"
                           f"insert into profileproperties "
                           f"(SELECT subdoelgroep.profileid, doelgroep, category, sub_category, brand, "
                           f"herhaalaankopen, pricecategory "
                           f"from subdoelgroep left join subcategory "
                           f"on subdoelgroep.profileid=subcategory.profileid "
                           f"left join subsubcategory on subdoelgroep.profileid=subsubcategory.profileid "
                           f"left join subbrand on subdoelgroep.profileid=subbrand.profileid "
                           f"left join subherhaal on subdoelgroep.profileid=subherhaal.profileid "
                           f"left join subprice on subdoelgroep.profileid=subprice.profileid)")


def createshortcut(cursor):
    P.executequery(cursor, f"drop table if exists orderedprofiles")
    P.executequery(cursor, f"create table orderedprofiles( profilesprofileid varchar, "
                           f"orderedproductid varchar, FOREIGN KEY (profilesprofileid) references profiles(profileid), "
                           f"FOREIGN KEY (orderedproductid) references products(productid))")
    P.executequery(cursor, f"insert into orderedprofiles (select profileid, productsproductid from orderedproducts "
                           f"left join sessions on sessionssessionid=sessionid "
                           f"left join profiles on profilesprofileid=profileid)")


connection = P.makeconnection('localhost', 'huwebshop', 'postgres', '1234')
cursor = P.makecursor(connection)
maketable(cursor)
filltable(cursor)
createshortcut(cursor)
connection.commit()
P.closeconnection(connection, cursor)
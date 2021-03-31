DROP TABLE IF EXISTS prodCombinations;
CREATE TABLE prodCombinations (combid SERIAL, doelgroep int,
							  category int, sub_category int,
							  price varchar(255), brand int, herhaalaankopen BIT,
							  PRIMARY KEY (combid),
							  FOREIGN KEY (doelgroep) REFERENCES doelgroepen(doelgroepid),
							  FOREIGN KEY (category) REFERENCES categories(categoryid),
							  FOREIGN KEY (sub_category) REFERENCES sub_categories(sub_categoryid),
							  FOREIGN KEY (brand) REFERENCES brands(brandid));

INSERT INTO prodCombinations (doelgroep, category, sub_category, price, brand, herhaalaankopen)
SELECT distinct doelgroependoelgroepid, category, sub_categoriessub_categoryid, pr AS price,
				brandsbrandid, herhaalaankopen
FROM (SELECT distinct categoriescategoryid AS category, sub_categoriessub_categoryid, doelgroependoelgroepid,
				brandsbrandid, herhaalaankopen, price,
CASE
	WHEN price < 250 THEN  'low'
	WHEN price >= 250 AND price < 600 THEN 'mid'
	ELSE 'high'
END AS pr
FROM products) AS combi


DROP TABLE IF EXISTS prodcomb;
CREATE TABLE prodcomb (productid varchar(255), combid int,
					   FOREIGN KEY (productid) REFERENCES products(productid),
					   FOREIGN KEY (combid) REFERENCES prodcombinations(combid));

INSERT INTO prodcomb (productid, combid)
SELECT pr, comb FROM
(SELECT prodcombinations, products.productid, products.price, products.herhaalaankopen,
	   products.brandsbrandid, products.categoriescategoryid, products.sub_categoriessub_categoryid,
	   products.doelgroependoelgroepid,
CASE
	WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid
	    OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL)
	AND (prodcombinations.category = products.categoriescategoryid
		 OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL)
	AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid
 		OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid IS NULL)
	AND (prodcombinations.brand = products.brandsbrandid
 		OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL)
	AND (prodcombinations.herhaalaankopen = products.herhaalaankopen
		OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL)
 	AND prodcombinations.price = 'low'
 	AND products.price < 250
	THEN products.productid
 	WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid
 		OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL)
	AND (prodcombinations.category = products.categoriescategoryid
		 OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL)
	AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid
 		OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid IS NULL)
	AND (prodcombinations.brand = products.brandsbrandid
 		OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL)
	AND (prodcombinations.herhaalaankopen = products.herhaalaankopen
		OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL)
 	AND prodcombinations.price = 'mid'
 	AND products.price >= 250 AND products.price < 600
	THEN products.productid
 	WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid
 		OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL)
	AND (prodcombinations.category = products.categoriescategoryid
		 OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL)
	AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid
 		OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid IS NULL)
	AND (prodcombinations.brand = products.brandsbrandid
 		OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL)
	AND (prodcombinations.herhaalaankopen = products.herhaalaankopen
		OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL)
 	AND prodcombinations.price = 'high'
 	AND products.price >= 600
	THEN products.productid
END AS pr,
CASE
	WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid
 		OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL)
	AND (prodcombinations.category = products.categoriescategoryid
		 OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL)
	AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid
 		OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid IS NULL)
	AND (prodcombinations.brand = products.brandsbrandid
 		OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL)
	AND (prodcombinations.herhaalaankopen = products.herhaalaankopen
		OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL)
 	AND prodcombinations.price = 'low'
 	AND products.price < 250
	THEN prodcombinations.combid
 	WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid
 		OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL)
	AND (prodcombinations.category = products.categoriescategoryid
		 OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL)
	AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid
 		OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid IS NULL)
	AND (prodcombinations.brand = products.brandsbrandid
 		OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL)
	AND (prodcombinations.herhaalaankopen = products.herhaalaankopen
		OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL)
 	AND prodcombinations.price = 'mid'
 	AND products.price >= 250 AND products.price < 600
	THEN prodcombinations.combid
 	WHEN (prodcombinations.doelgroep = products.doelgroependoelgroepid
 		OR prodcombinations.doelgroep IS NULL AND products.doelgroependoelgroepid IS NULL)
	AND (prodcombinations.category = products.categoriescategoryid
		 OR prodcombinations.category IS NULL AND products.categoriescategoryid IS NULL)
	AND (prodcombinations.sub_category = products.sub_categoriessub_categoryid
 		OR prodcombinations.sub_category IS NULL AND products.sub_categoriessub_categoryid IS NULL)
	AND (prodcombinations.brand = products.brandsbrandid
 		OR prodcombinations.brand IS NULL AND products.brandsbrandid IS NULL)
	AND (prodcombinations.herhaalaankopen = products.herhaalaankopen
		OR prodcombinations.herhaalaankopen IS NULL AND products.herhaalaankopen IS NULL)
 	AND prodcombinations.price = 'high'
 	AND products.price >= 600
	THEN prodcombinations.combid
END AS comb
FROM prodcombinations, products) as prod
WHERE pr IS NOT NULL AND comb IS NOT NULL
ORDER BY comb

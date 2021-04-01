DROP TABLE IF EXISTS popular;
CREATE TABLE popular(id SERIAL, productid varchar(255), pop int,
					PRIMARY KEY (id),
					FOREIGN KEY (productid) REFERENCES products(productid));
INSERT INTO popular (productid, pop)
SELECT topsold.productid, (topsold.id + topviewed.id) as pop
FROM topsold, topviewed
WHERE topsold.productid = topviewed.productid
ORDER BY pop
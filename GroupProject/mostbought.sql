DROP TABLE IF EXISTS TopSold;
CREATE TABLE TopSold(top4id SERIAL, productid varchar(255) NOT NULL, freq bigint NOT NULL,
					  PRIMARY KEY (top4id),
					  FOREIGN KEY (productid) REFERENCES products (productid));

INSERT INTO TopSold (productid, freq)
SELECT productsproductid, count(productsproductid)
FROM orderedproducts
GROUP by productsproductid
ORDER BY count DESC
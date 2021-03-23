DROP TABLE IF EXISTS Top4Sold;
CREATE TABLE Top4Sold(top4id SERIAL, productid varchar(255) NOT NULL, freq bigint NOT NULL,
					  PRIMARY KEY (top4id),
					  FOREIGN KEY (productid) REFERENCES products (productid));

INSERT INTO Top4Sold (productid, freq)
SELECT productsproductid, count(productsproductid)
FROM orderedproducts
GROUP by productsproductid
ORDER BY count DESC
DROP TABLE IF EXISTS TopViewed;
CREATE TABLE TopViewed(id SERIAL, productid varchar(255) NOT NULL, freq bigint NOT NULL,
					  PRIMARY KEY (id),
					  FOREIGN KEY (productid) REFERENCES products (productid));

INSERT INTO TopViewed (productid, freq)
SELECT productsproductid, count(productsproductid)
FROM viewedproducts
WHERE productsproductid IS NOT NULL
GROUP by productsproductid
ORDER BY count DESC
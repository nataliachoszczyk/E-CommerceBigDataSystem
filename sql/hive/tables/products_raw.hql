CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

CREATE EXTERNAL TABLE IF NOT EXISTS products_raw (
    product_id BIGINT,
    name STRING,
    category_code STRING,
    brand STRING,
    price DOUBLE,
    stock INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/products'
TBLPROPERTIES ("skip.header.line.count"="1");

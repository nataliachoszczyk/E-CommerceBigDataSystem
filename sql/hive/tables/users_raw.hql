CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;

CREATE EXTERNAL TABLE IF NOT EXISTS users_raw (
    user_id BIGINT,
    name STRING,
    surname STRING,
    birth_date DATE,
    city STRING,
    zip_code STRING,
    street_name STRING,
    house_number STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/raw/users'
TBLPROPERTIES ("skip.header.line.count"="1");

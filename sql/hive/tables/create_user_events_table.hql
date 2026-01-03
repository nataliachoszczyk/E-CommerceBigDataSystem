-- Tabela Hive dla eventów użytkowników
-- Tabela zewnętrzna, partycjonowana według czasu
-- Dane w formacie Parquet z kompresją Snappy

USE default;

DROP TABLE IF EXISTS user_events;

CREATE EXTERNAL TABLE user_events (
    event_time STRING,
    event_type STRING,
    product_id BIGINT,
    category_id STRING,             
    category_code STRING,
    brand STRING,
    price DOUBLE,
    user_id BIGINT,
    user_session STRING,
    original_time STRING,           
    mapped_time STRING,              
    processing_time STRING,         
    kafka_timestamp BIGINT,
    kafka_partition BIGINT
)
PARTITIONED BY (
    year INT,
    month INT,
    day INT,
    hour INT
)
STORED AS PARQUET
LOCATION '/raw/events'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY'
);

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

MSCK REPAIR TABLE user_events;

SHOW PARTITIONS user_events;

DESCRIBE FORMATTED user_events;

#!/usr/bin/env python3
"""
Konsumuje eventy z Kafki i zapisuje je do HDFS w formacie Parquet
ZOPTYMALIZOWANY - przetwarzanie w batches, niskie zużycie RAM
"""

import sys
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_spark_session():
    """Tworzy sesję Spark z odpowiednią konfiguracją."""
    return SparkSession.builder \
        .appName("KafkaToHDFSArchiver") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


def get_schema():
    """Definiuje schemat dla eventów użytkowników."""
    return StructType([
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", StringType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
        StructField("original_time", StringType(), True),
        StructField("mapped_time", StringType(), True),
        StructField("processing_time", StringType(), True),
        StructField("kafka_timestamp", LongType(), True),
        StructField("kafka_partition", LongType(), True)
    ])


def consume_and_archive_batched(start_time, end_time, output_path):
    """
    Konsumuje eventy z Kafki i zapisuje w HDFS.
    
    Args:
        start_time: datetime - początek przedziału
        end_time: datetime - koniec przedziału
        output_path: str - ścieżka bazowa w HDFS
    """
    logger.info(f"Archiwizacja eventów: {start_time} - {end_time}")
    
    consumer = KafkaConsumer(
        'user-events',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        group_id=f'hdfs-archiver-{start_time.strftime("%Y%m%d%H")}',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=30000
    )
    
    BATCH_SIZE = 250000 
    batch = []
    batch_num = 0
    total_events = 0
    
    start_timestamp = int(start_time.timestamp() * 1000)
    end_timestamp = int(end_time.timestamp() * 1000)
    
    logger.info(f"Szukam eventów z przedziału timestamp: {start_timestamp} - {end_timestamp}")
    logger.info(f"Batch size: {BATCH_SIZE} eventów")
    
    spark = create_spark_session()
    schema = get_schema()
    
    full_output_path = f"{output_path}/year={start_time.year}/month={start_time.month:02d}/day={start_time.day:02d}/hour={start_time.hour:02d}"
    
    try:
        for message in consumer:
            kafka_timestamp = message.timestamp
            
            if start_timestamp <= kafka_timestamp < end_timestamp:
                event = message.value
                event['kafka_timestamp'] = kafka_timestamp
                event['kafka_partition'] = message.partition
                batch.append(event)
                total_events += 1
                
                if total_events % 10000 == 0:
                    logger.info(f"Zebrano {total_events} eventów...")
                
                if len(batch) >= BATCH_SIZE:
                    batch_num += 1
                    logger.info(f"Zapisuję batch #{batch_num} ({len(batch)} eventów)...")
                    
                    df = spark.createDataFrame(batch, schema=schema)

                    mode = "overwrite" if batch_num == 1 else "append"
                    df.write.mode(mode).parquet(full_output_path)
                    
                    logger.info(f"Batch #{batch_num} zapisany ({len(batch)} eventów)")

                    batch = []
                    df.unpersist()
                    del df
            
            elif kafka_timestamp >= end_timestamp:
                logger.info(f"Osiągnięto koniec przedziału czasowego")
                break
        
        if batch:
            batch_num += 1
            logger.info(f"Zapisuję ostatni batch #{batch_num} ({len(batch)} eventów)...")
            
            df = spark.createDataFrame(batch, schema=schema)
            mode = "overwrite" if batch_num == 1 else "append"
            df.write.mode(mode).parquet(full_output_path)
            
            logger.info(f"Ostatni batch zapisany ({len(batch)} eventów)")
            batch = []
    
    except Exception as e:
        logger.error(f"Błąd podczas konsumowania: {e}")
        raise
    finally:
        consumer.close()
        spark.stop()
    
    logger.info(f"Zebrano łącznie {total_events} eventów w {batch_num} batches")
    logger.info(f"Pomyślnie zapisano do HDFS: {full_output_path}")
    
    return total_events


def main():
    """Główna funkcja."""
    if len(sys.argv) != 2:
        print("Usage: python3 kafka_to_hdfs_archiver.py <archive_hour_datetime>")
        print("Example: python3 kafka_to_hdfs_archiver.py '2025-12-27 00:00:00'")
        sys.exit(1)
    
    try:
        archive_datetime = datetime.strptime(sys.argv[1], "%Y-%m-%d %H:%M:%S")
    except ValueError:
        logger.error("Nieprawidłowy format daty. Użyj: YYYY-MM-DD HH:MM:SS")
        sys.exit(1)
    
    start_time = archive_datetime
    end_time = start_time + timedelta(hours=1)
    
    hdfs_base_path = "hdfs://localhost:9000/raw/events"
    
    events_archived = consume_and_archive_batched(start_time, end_time, hdfs_base_path)
    
    logger.info(f"Archiwizacja zakończona. Zapisano {events_archived} eventów.")
    
    if events_archived > 0:
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()

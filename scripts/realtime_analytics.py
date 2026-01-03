#!/usr/bin/env python3
"""
Analizuje eventy z Kafki w 5-minutowych oknach czasowych:
- Liczba aktywnych użytkowników
- Top 10 produktów (wyświetlenia)
- Top 10 produktów (zakupy)
- Średnia wartość koszyka
- Rozkład typów eventów

Wyniki zapisywane do HBase w tabeli realtime_stats.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import happybase
from datetime import datetime
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

HBASE_HOST = 'localhost'
HBASE_PORT = 9090


def create_spark_session():
    """Tworzy sesję Spark z konfiguracją dla streaming."""
    return SparkSession.builder \
        .appName("RealtimeEcommerceAnalytics") \
        .master("local[4]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/home/win10/spark-checkpoints/realtime") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()


def get_event_schema():
    """Schemat eventów z Kafki - dokładnie jak w JSON."""
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
        StructField("processing_time", StringType(), True)
    ])


def write_to_hbase_active_users(df, epoch_id):
    """
    Zapisuje metryki aktywnych użytkowników do HBase: realtime_stats.
    """
    logger.info(f"[EPOCH {epoch_id}] Zapisuję active_users do HBase...")
    
    if df.isEmpty():
        logger.warning(f"[EPOCH {epoch_id}] Brak danych active_users - pomijam")
        return
    
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        table = connection.table('realtime_stats')
        
        records = df.collect()
        written = 0
        
        for row in records:
            window_start = row['window_start']
            window_end = row['window_end']
            active_users = row['active_users']
            total_events = row['total_events']
            
            # Row key: YYYYMMDD_HHMM_active_users
            timestamp_key = window_start.strftime('%Y%m%d_%H%M')
            row_key = f"{timestamp_key}_active_users".encode('utf-8')
            
            table.put(row_key, {
                b'metrics:active_users': str(active_users).encode('utf-8'),
                b'metrics:total_events': str(total_events).encode('utf-8'),
                b'metadata:window_start': window_start.isoformat().encode('utf-8'),
                b'metadata:window_end': window_end.isoformat().encode('utf-8'),
                b'metadata:created_at': datetime.now().isoformat().encode('utf-8')
            })
            written += 1
        
        connection.close()
        logger.info(f"[EPOCH {epoch_id}] Zapisano {written} rekordów active_users do HBase")
        
    except Exception as e:
        logger.error(f"[EPOCH {epoch_id}] Błąd zapisu active_users do HBase: {e}")
        output_path = f"/tmp/realtime_stats/active_users/batch_{epoch_id}"
        df.write.mode("overwrite").json(output_path)
        logger.info(f"[EPOCH {epoch_id}] Fallback: zapisano do {output_path}")


def write_to_hbase_top_products_views(df, epoch_id):
    """
    Zapisuje top produkty (views) do HBase: realtime_stats.
    Row key: timestamp_top_view_product_PRODUCT_ID
    """
    logger.info(f"[EPOCH {epoch_id}] Zapisuję top_products_views do HBase...")
    
    if df.isEmpty():
        logger.warning(f"[EPOCH {epoch_id}] Brak danych top_products_views - pomijam")
        return
    
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        table = connection.table('realtime_stats')
    
        top10 = df.orderBy(desc("view_count")).limit(10).collect()
        written = 0
        
        for rank, row in enumerate(top10, start=1):
            window_start = row['window_start']
            product_id = row['product_id']
            brand = row['brand'] if row['brand'] else 'unknown'
            category = row['category_code'] if row['category_code'] else 'unknown'
            view_count = row['view_count']
            avg_price = row["avg_price"] if row["avg_price"] is not None else 0.0
            timestamp_key = window_start.strftime('%Y%m%d_%H%M')
            row_key = f"{timestamp_key}_top_view_{rank:02d}_{product_id}".encode('utf-8')
            
            table.put(row_key, {
                b'metrics:rank': str(rank).encode('utf-8'),
                b'metrics:product_id': str(product_id).encode('utf-8'),
                b'metrics:brand': brand.encode('utf-8'),
                b'metrics:category': category.encode('utf-8'),
                b'metrics:view_count': str(view_count).encode('utf-8'),
                b'metrics:avg_price': str(f"{avg_price:.2f}").encode('utf-8'),
                b'metadata:window_start': window_start.isoformat().encode('utf-8'),
                b'metadata:created_at': datetime.now().isoformat().encode('utf-8')
            })
            written += 1
        
        connection.close()
        logger.info(f"[EPOCH {epoch_id}] Zapisano {written} rekordów top_products_views do HBase")
        
    except Exception as e:
        logger.error(f"[EPOCH {epoch_id}] Błąd zapisu top_products_views do HBase: {e}")
        output_path = f"/tmp/realtime_stats/top_products_views/batch_{epoch_id}"
        df.write.mode("overwrite").json(output_path)
        logger.info(f"[EPOCH {epoch_id}] Fallback: zapisano do {output_path}")


def write_to_hbase_purchases(df, epoch_id):
    """
    Zapisuje metryki zakupów do HBase: realtime_stats.
    Row key: timestamp_purchases
    """
    logger.info(f"[EPOCH {epoch_id}] Zapisuję purchases do HBase...")
    
    if df.isEmpty():
        logger.warning(f"[EPOCH {epoch_id}] Brak danych purchases - pomijam")
        return
    
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        table = connection.table('realtime_stats')
        
        records = df.collect()
        written = 0
        
        for row in records:
            window_start = row['window_start']
            purchase_count = row['purchase_count']
            total_revenue = row["total_revenue"] if row["total_revenue"] is not None else 0.0
            avg_basket = row["avg_basket_value"] if row["avg_basket_value"] is not None else 0.0
            purchasing_users = row['purchasing_users']
            
            timestamp_key = window_start.strftime('%Y%m%d_%H%M')
            row_key = f"{timestamp_key}_purchases".encode('utf-8')
            
            table.put(row_key, {
                b'metrics:purchase_count': str(purchase_count).encode('utf-8'),
                b'metrics:total_revenue': str(f"{total_revenue:.2f}").encode('utf-8'),
                b'metrics:avg_basket_value': str(f"{avg_basket:.2f}").encode('utf-8'),
                b'metrics:purchasing_users': str(purchasing_users).encode('utf-8'),
                b'metadata:window_start': window_start.isoformat().encode('utf-8'),
                b'metadata:created_at': datetime.now().isoformat().encode('utf-8')
            })
            written += 1
        
        connection.close()
        logger.info(f"[EPOCH {epoch_id}] Zapisano {written} rekordów purchases do HBase")
        
    except Exception as e:
        logger.error(f"[EPOCH {epoch_id}] Błąd zapisu purchases do HBase: {e}")
        output_path = f"/tmp/realtime_stats/purchases/batch_{epoch_id}"
        df.write.mode("overwrite").json(output_path)
        logger.info(f"[EPOCH {epoch_id}] Fallback: zapisano do {output_path}")


def write_to_hbase_event_distribution(df, epoch_id):
    """
    Zapisuje rozkład typów eventów do HBase: realtime_stats.
    Row key: timestamp_event_EVENT_TYPE
    """
    logger.info(f"[EPOCH {epoch_id}] Zapisuję event_distribution do HBase...")
    
    if df.isEmpty():
        logger.warning(f"[EPOCH {epoch_id}] Brak danych event_distribution - pomijam")
        return
    
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        table = connection.table('realtime_stats')
        
        records = df.collect()
        written = 0
        
        for row in records:
            window_start = row['window_start']
            event_type = row['event_type']
            event_count = row['event_count']
            
            timestamp_key = window_start.strftime('%Y%m%d_%H%M')
            row_key = f"{timestamp_key}_event_{event_type}".encode('utf-8')
            
            table.put(row_key, {
                b'metrics:event_type': event_type.encode('utf-8'),
                b'metrics:event_count': str(event_count).encode('utf-8'),
                b'metadata:window_start': window_start.isoformat().encode('utf-8'),
                b'metadata:created_at': datetime.now().isoformat().encode('utf-8')
            })
            written += 1
        
        connection.close()
        logger.info(f"[EPOCH {epoch_id}] Zapisano {written} rekordów event_distribution do HBase")
        
    except Exception as e:
        logger.error(f"[EPOCH {epoch_id}] Błąd zapisu event_distribution do HBase: {e}")
        output_path = f"/tmp/realtime_stats/event_distribution/batch_{epoch_id}"
        df.write.mode("overwrite").json(output_path)
        logger.info(f"[EPOCH {epoch_id}] Fallback: zapisano do {output_path}")


def write_to_hbase_top_brands(df, epoch_id):
    """
    Zapisuje top marki do HBase: realtime_stats.
    Row key: timestamp_brand_RANK_BRAND_EVENT_TYPE
    """
    logger.info(f"[EPOCH {epoch_id}] Zapisuję top_brands do HBase...")
    
    if df.isEmpty():
        logger.warning(f"[EPOCH {epoch_id}] Brak danych top_brands - pomijam")
        return

    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        table = connection.table('realtime_stats')
        top10 = df.orderBy(col("event_count").desc()).limit(10).collect()
        written = 0
        
        for rank, row in enumerate(top10, start=1):
            window_start = row['window_start']
            brand = row['brand']
            event_type = row['event_type']
            event_count = row['event_count']
            total_value = row['total_value'] if row['total_value'] else 0.0
            timestamp_key = window_start.strftime('%Y%m%d_%H%M')
            row_key = f"{timestamp_key}_brand_{rank:02d}_{brand}_{event_type}".encode('utf-8')
            table.put(row_key, {
                b'metrics:rank': str(rank).encode('utf-8'),
                b'metrics:brand': brand.encode('utf-8'),
                b'metrics:event_type': event_type.encode('utf-8'),
                b'metrics:event_count': str(event_count).encode('utf-8'),
                b'metrics:total_value': str(f"{total_value:.2f}").encode('utf-8'),
                b'metadata:window_start': window_start.isoformat().encode('utf-8'),
                b'metadata:created_at': datetime.now().isoformat().encode('utf-8')
            })
            written += 1
        
        connection.close()
        logger.info(f"[EPOCH {epoch_id}] Zapisano {written} rekordów top_brands do HBase")
        
    except Exception as e:
        logger.error(f"[EPOCH {epoch_id}] Błąd zapisu top_brands do HBase: {e}")
        output_path = f"/tmp/realtime_stats/top_brands/batch_{epoch_id}"
        df.write.mode("overwrite").json(output_path)
        logger.info(f"[EPOCH {epoch_id}] Fallback: zapisano do {output_path}")

def main():
    """Główna funkcja analityczna."""
    logger.info("=== URUCHAMIAM REAL-TIME ANALYTICS Z INTEGRACJĄ HBASE ===")
    
    try:
        test_conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        test_conn.open()
        tables = [t.decode('utf-8') for t in test_conn.tables()]
        logger.info(f"HBase połączenie OK, tabele: {tables}")
        test_conn.close()
        
        if 'realtime_stats' not in tables:
            logger.error("Tabela realtime_stats nie istnieje!")
            logger.error("Uruchom najpierw: python3 hbase_schema_setup.py")
            return
            
    except Exception as e:
        logger.error(f"Błąd połączenia z HBase: {e}")
        logger.error("Sprawdź czy HBase Thrift Server działa: hbase thrift start")
        return
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "user-events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    schema = get_event_schema()
    events_df = kafka_df \
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ) \
        .select("data.*", "kafka_timestamp")
    
    events_with_time = events_df \
        .withColumn("event_timestamp", to_timestamp(col("event_time")))
    
    # === ANALIZA 1: Aktywni użytkownicy ===
    active_users = events_with_time \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(window(col("event_timestamp"), "5 minutes", "1 minute")) \
        .agg(
            approx_count_distinct("user_id").alias("active_users"), 
            count("*").alias("total_events")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("active_users"),
            col("total_events")
        )
    
    query1 = active_users \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, epoch: write_to_hbase_active_users(df, epoch)) \
        .start()
    
    # === ANALIZA 2: Top produkty (views) ===
    top_products_views = events_with_time \
        .filter(col("event_type") == "view") \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),
            col("product_id"),
            col("brand"),
            col("category_code")
        ) \
        .agg(
            count("*").alias("view_count"),
            avg(col("price")).alias("avg_price")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            col("brand"),
            col("category_code"),
            col("view_count"),
            col("avg_price")
        )
    
    query2 = top_products_views \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, epoch: write_to_hbase_top_products_views(df, epoch)) \
        .start()
    
    # === ANALIZA 3: Zakupy ===
    purchases = events_with_time \
        .filter(col("event_type") == "purchase") \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(window(col("event_timestamp"), "5 minutes", "1 minute")) \
        .agg(
            count("*").alias("purchase_count"),
            sum(col("price")).alias("total_revenue"),
            avg(col("price")).alias("avg_basket_value"),
            approx_count_distinct("user_id").alias("purchasing_users")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("purchase_count"),
            col("total_revenue"),
            col("avg_basket_value"),
            col("purchasing_users")
        )
    
    query3 = purchases \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, epoch: write_to_hbase_purchases(df, epoch)) \
        .start()
    
    # === ANALIZA 4: Rozkład eventów ===
    event_distribution = events_with_time \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),
            col("event_type")
        ) \
        .agg(count("*").alias("event_count")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("event_type"),
            col("event_count")
        )
    
    query4 = event_distribution \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, epoch: write_to_hbase_event_distribution(df, epoch)) \
        .start()
    
    # === ANALIZA 5: Top marki ===
    top_brands = events_with_time \
        .filter(col("brand").isNotNull()) \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window(col("event_timestamp"), "5 minutes", "1 minute"),
            col("brand"),
            col("event_type")
        ) \
        .agg(
            count("*").alias("event_count"),
            sum(col("price")).alias("total_value")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("brand"),
            col("event_type"),
            col("event_count"),
            col("total_value")
        )
    
    query5 = top_brands \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(lambda df, epoch: write_to_hbase_top_brands(df, epoch)) \
        .start()
    
    logger.info("  Wszystkie streamy uruchomione i zapisują do HBase!")
    logger.info("  Monitoruj: hbase shell → scan 'realtime_stats', {LIMIT => 10}")
    logger.info("  Ctrl+C aby zakończyć")
    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Otrzymano sygnał przerwania. Zamykam...")
    except Exception as e:
        logger.error(f"Błąd: {e}", exc_info=True)
        raise

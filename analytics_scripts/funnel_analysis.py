#!/usr/bin/env python3
"""
Analiza lejka zakupowego (Funnel Analysis)
- Przetwarza dane z ostatnich 24 godzin z tabeli user_events (bazując na mapped_time)
- Ścieżka: view → cart → purchase
- Oblicza średni czas między etapami
- Oblicza drop-off rate według kategorii i marek
- Zapisuje wyniki do HBase: funnel_analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
import happybase
from datetime import datetime, timedelta
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

HBASE_HOST = 'localhost'
HBASE_PORT = 9090


def create_spark_session():
    """Tworzy sesję Spark dla batch processing."""
    return SparkSession.builder \
        .appName("FunnelAnalysis") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .enableHiveSupport() \
        .getOrCreate()


def get_events_last_24h(spark):
    """Wczytuje eventy z ostatnich 24 godzin z Hive (bazując na mapped_time)."""
    logger.info("Wczytuję eventy z ostatnich 24 godzin (bazując na mapped_time)...")
    
    # Oblicz zakres czasu dla mapped_time
    now = datetime.now()
    time_24h_ago = now - timedelta(hours=24)
    
    logger.info(f"Zakres mapped_time: {time_24h_ago} -> {now}")
    
    # Wczytaj WSZYSTKIE dostępne dane (nie filtruj po partycjach year/month/day)
    # Te partycje to czas archiwizacji, nie czas eventu
    df = spark.sql("""
        SELECT 
            event_time,
            mapped_time,
            event_type,
            product_id,
            category_id,
            category_code,
            brand,
            price,
            user_id,
            user_session
        FROM user_events
    """)
    
    logger.info(f"Wczytano {df.count()} rekordów ze wszystkich partycji")
    
    # Konwertuj mapped_time na timestamp
    df = df.withColumn("timestamp", to_timestamp("mapped_time"))
    
    # Teraz filtruj po mapped_time (rzeczywisty czas eventu)
    df = df.filter(col("timestamp") >= lit(time_24h_ago))
    df = df.filter(col("timestamp") <= lit(now))
    
    count = df.count()
    logger.info(f"Po filtracji mapped_time ostatnich 24h: {count} eventów")
    
    # Pokaż rozkład typów eventów
    logger.info("Rozkład typów eventów:")
    df.groupBy("event_type").count().show()
    
    return df


def safe_float(value, decimals=4):
    """Bezpieczna konwersja wartości na string z obsługą None."""
    if value is None:
        return "0"
    try:
        return str(round(float(value), decimals))
    except:
        return "0"


def safe_int(value):
    """Bezpieczna konwersja wartości na string int z obsługą None."""
    if value is None:
        return "0"
    try:
        return str(int(value))
    except:
        return "0"


def calculate_funnel_metrics(df):
    """
    Oblicza metryki lejka zakupowego:
    - Liczba użytkowników na każdym etapie (view, cart, purchase)
    - Średni czas między etapami
    - Drop-off rate według kategorii i marek
    """
    logger.info("Obliczam metryki funnel...")
    
    # 1. Agreguj eventy według user_id, product_id, category, brand
    user_product_events = df.groupBy(
        "user_id", "product_id", "category_code", "brand"
    ).agg(
        min(when(col("event_type") == "view", col("timestamp"))).alias("view_time"),
        min(when(col("event_type") == "cart", col("timestamp"))).alias("cart_time"),
        min(when(col("event_type") == "purchase", col("timestamp"))).alias("purchase_time"),
        countDistinct(when(col("event_type") == "view", 1)).alias("has_view"),
        countDistinct(when(col("event_type") == "cart", 1)).alias("has_cart"),
        countDistinct(when(col("event_type") == "purchase", 1)).alias("has_purchase")
    )
    
    # 2. Oblicz czasy między etapami (w sekundach)
    user_product_events = user_product_events.withColumn(
        "view_to_cart_seconds",
        when(col("cart_time").isNotNull() & col("view_time").isNotNull(),
             (unix_timestamp("cart_time") - unix_timestamp("view_time")))
    ).withColumn(
        "cart_to_purchase_seconds",
        when(col("purchase_time").isNotNull() & col("cart_time").isNotNull(),
             (unix_timestamp("purchase_time") - unix_timestamp("cart_time")))
    ).withColumn(
        "view_to_purchase_seconds",
        when(col("purchase_time").isNotNull() & col("view_time").isNotNull(),
             (unix_timestamp("purchase_time") - unix_timestamp("view_time")))
    )
    
    # 3. Agreguj według kategorii
    category_funnel = user_product_events.groupBy("category_code").agg(
        countDistinct(when(col("has_view") > 0, col("user_id"))).alias("users_viewed"),
        countDistinct(when(col("has_cart") > 0, col("user_id"))).alias("users_added_to_cart"),
        countDistinct(when(col("has_purchase") > 0, col("user_id"))).alias("users_purchased"),
        avg("view_to_cart_seconds").alias("avg_view_to_cart_seconds"),
        avg("cart_to_purchase_seconds").alias("avg_cart_to_purchase_seconds"),
        avg("view_to_purchase_seconds").alias("avg_view_to_purchase_seconds")
    )
    
    # Oblicz conversion rates i drop-off rates
    category_funnel = category_funnel.withColumn(
        "view_to_cart_rate",
        when(col("users_viewed") > 0, col("users_added_to_cart") / col("users_viewed")).otherwise(0)
    ).withColumn(
        "cart_to_purchase_rate",
        when(col("users_added_to_cart") > 0, col("users_purchased") / col("users_added_to_cart")).otherwise(0)
    ).withColumn(
        "view_to_purchase_rate",
        when(col("users_viewed") > 0, col("users_purchased") / col("users_viewed")).otherwise(0)
    ).withColumn(
        "view_to_cart_dropoff",
        when(col("users_viewed") > 0, 1 - (col("users_added_to_cart") / col("users_viewed"))).otherwise(0)
    ).withColumn(
        "cart_to_purchase_dropoff",
        when(col("users_added_to_cart") > 0, 1 - (col("users_purchased") / col("users_added_to_cart"))).otherwise(0)
    )
    
    # 4. Agreguj według marek
    brand_funnel = user_product_events.filter(col("brand").isNotNull()).groupBy("brand").agg(
        countDistinct(when(col("has_view") > 0, col("user_id"))).alias("users_viewed"),
        countDistinct(when(col("has_cart") > 0, col("user_id"))).alias("users_added_to_cart"),
        countDistinct(when(col("has_purchase") > 0, col("user_id"))).alias("users_purchased"),
        avg("view_to_cart_seconds").alias("avg_view_to_cart_seconds"),
        avg("cart_to_purchase_seconds").alias("avg_cart_to_purchase_seconds"),
        avg("view_to_purchase_seconds").alias("avg_view_to_purchase_seconds")
    )
    
    brand_funnel = brand_funnel.withColumn(
        "view_to_cart_rate",
        when(col("users_viewed") > 0, col("users_added_to_cart") / col("users_viewed")).otherwise(0)
    ).withColumn(
        "cart_to_purchase_rate",
        when(col("users_added_to_cart") > 0, col("users_purchased") / col("users_added_to_cart")).otherwise(0)
    ).withColumn(
        "view_to_purchase_rate",
        when(col("users_viewed") > 0, col("users_purchased") / col("users_viewed")).otherwise(0)
    ).withColumn(
        "view_to_cart_dropoff",
        when(col("users_viewed") > 0, 1 - (col("users_added_to_cart") / col("users_viewed"))).otherwise(0)
    ).withColumn(
        "cart_to_purchase_dropoff",
        when(col("users_added_to_cart") > 0, 1 - (col("users_purchased") / col("users_added_to_cart"))).otherwise(0)
    )
    
    return category_funnel, brand_funnel


def write_to_hbase(category_df, brand_df):
    """Zapisuje wyniki do HBase w tabeli funnel_analytics (używa column families: segments, stats)."""
    logger.info("Zapisuję wyniki do HBase...")
    
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        table = connection.table('funnel_analytics')
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        written = 0
        
        # Zapisz metryki według kategorii
        logger.info("Zapisuję metryki według kategorii...")
        category_records = category_df.collect()
        for row in category_records:
            category = row['category_code'] if row['category_code'] else 'unknown'
            row_key = f"{timestamp}_category_{category}".encode('utf-8')
            
            table.put(row_key, {
                # Column family: stats (wszystkie metryki)
                b'stats:users_viewed': str(row['users_viewed']).encode('utf-8'),
                b'stats:users_added_to_cart': str(row['users_added_to_cart']).encode('utf-8'),
                b'stats:users_purchased': str(row['users_purchased']).encode('utf-8'),
                b'stats:view_to_cart_rate': safe_float(row['view_to_cart_rate']).encode('utf-8'),
                b'stats:cart_to_purchase_rate': safe_float(row['cart_to_purchase_rate']).encode('utf-8'),
                b'stats:view_to_purchase_rate': safe_float(row['view_to_purchase_rate']).encode('utf-8'),
                b'stats:view_to_cart_dropoff': safe_float(row['view_to_cart_dropoff']).encode('utf-8'),
                b'stats:cart_to_purchase_dropoff': safe_float(row['cart_to_purchase_dropoff']).encode('utf-8'),
                b'stats:avg_view_to_cart_seconds': safe_int(row['avg_view_to_cart_seconds']).encode('utf-8'),
                b'stats:avg_cart_to_purchase_seconds': safe_int(row['avg_cart_to_purchase_seconds']).encode('utf-8'),
                b'stats:avg_view_to_purchase_seconds': safe_int(row['avg_view_to_purchase_seconds']).encode('utf-8'),
                # Column family: segments (metadane)
                b'segments:type': b'category',
                b'segments:name': category.encode('utf-8'),
                b'segments:created_at': datetime.now().isoformat().encode('utf-8')
            })
            written += 1
        
        logger.info(f"Zapisano {written} rekordów dla kategorii")
        
        # Zapisz metryki według marek
        logger.info("Zapisuję metryki według marek...")
        brand_records = brand_df.collect()
        for row in brand_records:
            brand = row['brand']
            row_key = f"{timestamp}_brand_{brand}".encode('utf-8')
            
            table.put(row_key, {
                # Column family: stats (wszystkie metryki)
                b'stats:users_viewed': str(row['users_viewed']).encode('utf-8'),
                b'stats:users_added_to_cart': str(row['users_added_to_cart']).encode('utf-8'),
                b'stats:users_purchased': str(row['users_purchased']).encode('utf-8'),
                b'stats:view_to_cart_rate': safe_float(row['view_to_cart_rate']).encode('utf-8'),
                b'stats:cart_to_purchase_rate': safe_float(row['cart_to_purchase_rate']).encode('utf-8'),
                b'stats:view_to_purchase_rate': safe_float(row['view_to_purchase_rate']).encode('utf-8'),
                b'stats:view_to_cart_dropoff': safe_float(row['view_to_cart_dropoff']).encode('utf-8'),
                b'stats:cart_to_purchase_dropoff': safe_float(row['cart_to_purchase_dropoff']).encode('utf-8'),
                b'stats:avg_view_to_cart_seconds': safe_int(row['avg_view_to_cart_seconds']).encode('utf-8'),
                b'stats:avg_cart_to_purchase_seconds': safe_int(row['avg_cart_to_purchase_seconds']).encode('utf-8'),
                b'stats:avg_view_to_purchase_seconds': safe_int(row['avg_view_to_purchase_seconds']).encode('utf-8'),
                # Column family: segments (metadane)
                b'segments:type': b'brand',
                b'segments:name': brand.encode('utf-8'),
                b'segments:created_at': datetime.now().isoformat().encode('utf-8')
            })
            written += 1
        
        connection.close()
        logger.info(f"Łącznie zapisano {written} rekordów do HBase")
        
    except Exception as e:
        logger.error(f"Błąd zapisu do HBase: {e}")
        raise


def main():
    logger.info("=== START Funnel Analysis ===")
    
    try:
        # Utwórz sesję Spark
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Wczytaj dane z ostatnich 24h (bazując na mapped_time)
        events_df = get_events_last_24h(spark)
        
        if events_df.count() == 0:
            logger.warning("Brak danych z ostatnich 24h - kończę")
            return
        
        # Oblicz metryki funnel
        category_funnel, brand_funnel = calculate_funnel_metrics(events_df)
        
        # Pokaż przykładowe wyniki
        logger.info("\n=== TOP 10 Kategorii (według liczby wyświetleń) ===")
        category_funnel.orderBy(col("users_viewed").desc()).show(10, truncate=False)
        
        logger.info("\n=== TOP 10 Marek (według liczby wyświetleń) ===")
        brand_funnel.orderBy(col("users_viewed").desc()).show(10, truncate=False)
        
        # Zapisz do HBase
        write_to_hbase(category_funnel, brand_funnel)
        
        logger.info("=== KONIEC Funnel Analysis ===")
        
    except Exception as e:
        logger.error(f"Błąd w main: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

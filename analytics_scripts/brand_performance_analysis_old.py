#!/usr/bin/env python3
"""
Analiza efektywności marek (Brand Performance Analysis)
- Agreguje metryki według marek z ostatnich 7 dni
- Łączy z danymi demograficznymi użytkowników
- Zapisuje wyniki do HBase: brand_performance
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
        .appName("BrandPerformanceAnalysis") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .enableHiveSupport() \
        .getOrCreate()


def load_events(spark, days=7):
    """Wczytuje eventy z ostatnich N dni z Hive."""
    logger.info(f"Wczytuję eventy z ostatnich {days} dni...")
    
    now = datetime.now()
    time_ago = now - timedelta(days=days)
    
    df = spark.sql("""
        SELECT 
            user_id,
            product_id,
            brand,
            event_type,
            price,
            mapped_time
        FROM user_events
        WHERE brand IS NOT NULL
    """)
    
    df = df.withColumn("timestamp", to_timestamp("mapped_time"))
    df = df.filter(col("timestamp") >= lit(time_ago))
    
    count = df.count()
    logger.info(f"Wczytano {count} eventów z ostatnich {days} dni (z markami)")
    
    return df


def load_users(spark):
    """Wczytuje dane użytkowników z CSV w HDFS."""
    logger.info("Wczytuję dane użytkowników...")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("hdfs:///raw/users/users.csv")
    
    # Oblicz wiek
    df = df.withColumn("birth_date_parsed", to_date("birth_date", "yyyy-MM-dd"))
    df = df.withColumn("age", floor(datediff(current_date(), col("birth_date_parsed")) / 365.25))
    
    # Grupy wiekowe
    df = df.withColumn("age_group", 
        when(col("age") < 25, "18-24")
        .when((col("age") >= 25) & (col("age") < 35), "25-34")
        .when((col("age") >= 35) & (col("age") < 45), "35-44")
        .when((col("age") >= 45) & (col("age") < 55), "45-54")
        .otherwise("55+")
    )
    
    df = df.select("user_id", "age_group")
    
    count = df.count()
    logger.info(f"Wczytano {count} użytkowników")
    
    return df


def calculate_brand_performance(events_df, users_df):
    """Oblicza metryki wydajności dla każdej marki."""
    logger.info("Obliczam metryki wydajności marek...")
    
    # JOIN events + users
    logger.info("JOIN events + users...")
    df = events_df.join(users_df, "user_id", "inner")
    logger.info(f"Po JOIN: {df.count()} rekordów")
    
    # Agreguj metryki według marek
    brand_metrics = df.groupBy("brand").agg(
        countDistinct("user_id").alias("unique_users"),
        countDistinct(when(col("event_type") == "view", col("user_id"))).alias("viewers"),
        countDistinct(when(col("event_type") == "cart", col("user_id"))).alias("cart_users"),
        countDistinct(when(col("event_type") == "purchase", col("user_id"))).alias("buyers"),
        count(when(col("event_type") == "view", 1)).alias("total_views"),
        count(when(col("event_type") == "cart", 1)).alias("total_cart_adds"),
        count(when(col("event_type") == "purchase", 1)).alias("total_purchases"),
        sum(when(col("event_type") == "purchase", col("price"))).alias("total_revenue")
    )
    
    # Oblicz conversion rates
    brand_metrics = brand_metrics.withColumn(
        "view_to_cart_rate",
        when(col("viewers") > 0, col("cart_users") / col("viewers")).otherwise(0)
    ).withColumn(
        "cart_to_purchase_rate",
        when(col("cart_users") > 0, col("buyers") / col("cart_users")).otherwise(0)
    ).withColumn(
        "overall_conversion_rate",
        when(col("viewers") > 0, col("buyers") / col("viewers")).otherwise(0)
    )
    
    # Znajdź dominującą grupę wiekową dla każdej marki
    brand_age_groups = df.groupBy("brand", "age_group").agg(
        count("*").alias("events_count")
    )
    
    window = Window.partitionBy("brand").orderBy(col("events_count").desc())
    brand_age_groups = brand_age_groups.withColumn("rank", row_number().over(window))
    dominant_age_group = brand_age_groups.filter(col("rank") == 1).select("brand", "age_group")
    
    # JOIN z dominant age group
    brand_metrics = brand_metrics.join(dominant_age_group, "brand", "left")
    
    # Sortuj według revenue
    brand_metrics = brand_metrics.orderBy(col("total_revenue").desc())
    
    logger.info(f"Obliczono metryki dla {brand_metrics.count()} marek")
    
    return brand_metrics


def write_to_hbase(brand_metrics_df):
    """Zapisuje TOP 3 marki do HBase używając BATCH MODE."""
    logger.info("Zapisuję TOP 3 marki do HBase (batch mode)...")
    
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        table = connection.table('brand_performance')
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Zbierz TOP 3 marki
        brands = brand_metrics_df.limit(3).collect()
        
        # Użyj BATCH MODE - wydajniejsze
        with table.batch(batch_size=10) as batch:
            for row in brands:
                brand = row['brand']
                
                # Row key: timestamp_brand
                row_key = f"{timestamp}_brand_{brand}".encode('utf-8')
                
                logger.info(f"  Zapisuję: {brand}")
                
                # Przygotuj dane - UPROSZCZONE
                batch.put(row_key, {
                    # Column family: metrics (uproszczone)
                    b'metrics:brand': brand.encode('utf-8'),
                    b'metrics:unique_users': str(row['unique_users']).encode('utf-8'),
                    b'metrics:total_views': str(row['total_views']).encode('utf-8'),
                    b'metrics:total_purchases': str(row['total_purchases']).encode('utf-8'),
                    b'metrics:total_revenue': f"{row['total_revenue']:.2f}".encode('utf-8'),
                    b'metrics:conversion_rate': f"{row['overall_conversion_rate']:.4f}".encode('utf-8'),
                    b'metrics:dominant_age_group': (row['age_group'] if row['age_group'] else 'Unknown').encode('utf-8'),
                    b'metrics:created_at': datetime.now().isoformat().encode('utf-8'),
                    # Column family: trends
                    b'trends:period': b'7days',
                    b'trends:status': b'active'
                })
        
        connection.close()
        logger.info(f"✅ Zapisano {len(brands)} marek do HBase")
        
    except Exception as e:
        logger.error(f"❌ Błąd zapisu do HBase: {e}")
        raise


def main():
    logger.info("=== START Brand Performance Analysis ===")
    
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Wczytaj dane z ostatnich 7 dni
        events_df = load_events(spark, days=7)
        users_df = load_users(spark)
        
        if events_df.count() == 0:
            logger.warning("Brak eventów - kończę")
            return
        
        # Oblicz metryki
        brand_metrics = calculate_brand_performance(events_df, users_df)
        
        # Pokaż TOP 10 marek
        logger.info("\n=== TOP 10 Marek według przychodu ===")
        brand_metrics.select(
            "brand", "unique_users", "total_views", "total_purchases", 
            "total_revenue", "overall_conversion_rate", "age_group"
        ).show(10, truncate=False)
        
        # Zapisz TOP 3 do HBase
        write_to_hbase(brand_metrics)
        
        logger.info("=== KONIEC Brand Performance Analysis ===")
        
    except Exception as e:
        logger.error(f"Błąd w main: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

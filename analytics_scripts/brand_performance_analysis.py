#!/usr/bin/env python3
"""
Brand Performance Analysis - poprawny schemat HBase
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
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
    return SparkSession.builder \
        .appName("BrandPerformanceAnalysis") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .enableHiveSupport() \
        .getOrCreate()


def get_events_last_7d(spark):
    logger.info("Wczytuję eventy z ostatnich 7 dni...")
    
    now = datetime.now()
    time_7d_ago = now - timedelta(days=7)
    
    df = spark.sql("""
        SELECT 
            event_time,
            mapped_time,
            event_type,
            product_id,
            category_code,
            brand,
            price,
            user_id
        FROM user_events
        WHERE brand IS NOT NULL
    """)
    
    df = df.withColumn("timestamp", to_timestamp("mapped_time"))
    df = df.filter(col("timestamp") >= lit(time_7d_ago))
    
    count = df.count()
    logger.info(f"Wczytano {count} eventów z ostatnich 7 dni (z markami)")
    
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
    
    count = df.count()
    logger.info(f"Wczytano {count} użytkowników")
    
    return df.select("user_id", "age")


def calculate_brand_metrics(events_df, users_df):
    logger.info("Obliczam metryki wydajności marek...")
    
    # JOIN z użytkownikami
    logger.info("JOIN events + users...")
    df = events_df.join(users_df, "user_id", "left")
    logger.info(f"Po JOIN: {df.count()} rekordów")
    
    # Grupy wiekowe
    df = df.withColumn(
        "age_group",
        when(col("age") < 25, "18-24")
        .when(col("age") < 35, "25-34")
        .when(col("age") < 45, "35-44")
        .when(col("age") < 55, "45-54")
        .otherwise("55+")
    )
    
    # Agreguj według marki i grupy wiekowej
    brand_metrics = df.groupBy("brand", "age_group").agg(
        countDistinct("user_id").alias("unique_users"),
        sum(when(col("event_type") == "view", 1).otherwise(0)).alias("total_views"),
        sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
        sum(when(col("event_type") == "purchase", col("price")).otherwise(0)).alias("total_revenue")
    )
    
    # Conversion rate
    brand_metrics = brand_metrics.withColumn(
        "overall_conversion_rate",
        when(col("total_views") > 0, col("total_purchases") / col("total_views")).otherwise(0)
    )
    
    logger.info(f"Obliczono metryki dla {brand_metrics.select('brand').distinct().count()} marek")
    
    return brand_metrics


def write_to_hbase_top100(brand_metrics):
    """
    Zapisz tylko TOP 100 marek - POPRAWNY SCHEMAT
    """
    logger.info("Zapisuję TOP 100 marek do HBase...")
    
    # Weź TOP 100 według revenue
    top_brands = brand_metrics.groupBy("brand").agg(
        sum("total_revenue").alias("brand_total_revenue")
    ).orderBy(col("brand_total_revenue").desc()).limit(100)
    
    # Filtruj tylko te marki
    top_metrics = brand_metrics.join(
        top_brands.select("brand"), 
        "brand", 
        "inner"
    )
    
    records = top_metrics.collect()
    logger.info(f"Do zapisu: {len(records)} rekordów (TOP 100 marek)")
    
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=60000)
        table = connection.table('brand_performance')
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Zapisz pojedynczo - używaj metrics: i trends: (nie stats: i segments:)
        for i, row in enumerate(records):
            brand = row['brand']
            age_group = row['age_group']
            row_key = f"{timestamp}_{brand}_{age_group}".encode('utf-8')
            
            table.put(row_key, {
                b'metrics:unique_users': str(row['unique_users']).encode('utf-8'),
                b'metrics:total_views': str(row['total_views']).encode('utf-8'),
                b'metrics:total_purchases': str(row['total_purchases']).encode('utf-8'),
                b'metrics:total_revenue': f"{row['total_revenue']:.2f}".encode('utf-8'),
                b'metrics:conversion_rate': f"{row['overall_conversion_rate']:.4f}".encode('utf-8'),
                b'trends:brand': brand.encode('utf-8'),
                b'trends:age_group': age_group.encode('utf-8'),
                b'trends:created_at': datetime.now().isoformat().encode('utf-8')
            })
            
            if (i + 1) % 50 == 0:
                logger.info(f"  Zapisano {i + 1}/{len(records)} rekordów...")
        
        connection.close()
        logger.info(f"✅ Zapisano {len(records)} rekordów do HBase")
        
    except Exception as e:
        logger.error(f"❌ Błąd zapisu do HBase: {e}")
        raise


def main():
    logger.info("=== START Brand Performance Analysis ===")
    
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        events_df = get_events_last_7d(spark)
        users_df = load_users(spark)
        
        if events_df.count() == 0:
            logger.warning("Brak danych - kończę")
            return
        
        brand_metrics = calculate_brand_metrics(events_df, users_df)
        
        logger.info("\n=== TOP 10 Marek według przychodu ===")
        brand_metrics.orderBy(col("total_revenue").desc()).show(10, truncate=False)
        
        write_to_hbase_top100(brand_metrics)
        
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

#!/usr/bin/env python3
"""
Rekomendacje produktów według profilu demograficznego
- Łączy dane: events + users + products
- Segmentuje użytkowników według wieku i regionu
- Oblicza top 10 produktów dla każdego segmentu
- Zapisuje wyniki do HBase: personalized_recommendations
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
        .appName("DemographicRecommendations") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .enableHiveSupport() \
        .getOrCreate()


def load_events(spark):
    """Wczytuje eventy z ostatnich 24h z Hive."""
    logger.info("Wczytuję eventy z ostatnich 24h...")
    
    now = datetime.now()
    time_24h_ago = now - timedelta(hours=24)
    
    df = spark.sql("""
        SELECT 
            user_id,
            product_id,
            event_type,
            price,
            mapped_time
        FROM user_events
    """)
    
    df = df.withColumn("timestamp", to_timestamp("mapped_time"))
    df = df.filter(col("timestamp") >= lit(time_24h_ago))
    
    count = df.count()
    logger.info(f"Wczytano {count} eventów z ostatnich 24h")
    
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
    
    # Region - dla uproszczenia wszystko "Poland"
    df = df.withColumn("region", lit("Poland"))
    
    df = df.select("user_id", "city", "age", "age_group", "region")
    
    count = df.count()
    logger.info(f"Wczytano {count} użytkowników")
    
    return df


def load_products(spark):
    """Wczytuje katalog produktów z CSV w HDFS."""
    logger.info("Wczytuję katalog produktów...")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("hdfs:///raw/products/products.csv")
    
    df = df.select("product_id", "product_name", "category_name", "brand")
    
    count = df.count()
    logger.info(f"Wczytano {count} produktów")
    
    return df


def calculate_recommendations(events_df, users_df, products_df):
    """Oblicza rekomendacje dla segmentów demograficznych."""
    logger.info("Obliczam rekomendacje według segmentów...")
    
    # JOIN events + users
    logger.info("JOIN events + users...")
    df = events_df.join(users_df, "user_id", "inner")
    logger.info(f"Po JOIN: {df.count()} rekordów")
    
    # Znajdź aktywność segmentów (age_group)
    segment_activity = df.groupBy("age_group").agg(
        count("*").alias("events_count")
    ).orderBy(col("events_count").desc())
    
    logger.info("Segmenty według wieku:")
    segment_activity.show()
    
    # Agreguj metryki według age_group i produktu
    segment_product_stats = df.groupBy("age_group", "product_id").agg(
        countDistinct(when(col("event_type") == "view", col("user_id"))).alias("views"),
        countDistinct(when(col("event_type") == "cart", col("user_id"))).alias("add_to_cart"),
        countDistinct(when(col("event_type") == "purchase", col("user_id"))).alias("purchases"),
        sum(when(col("event_type") == "purchase", col("price"))).alias("total_revenue")
    )
    
    # Score: views + 2*cart + 5*purchases
    segment_product_stats = segment_product_stats.withColumn(
        "recommendation_score",
        col("views") + 2 * col("add_to_cart") + 5 * col("purchases")
    )
    
    # Ranking w każdym segmencie
    window = Window.partitionBy("age_group").orderBy(col("recommendation_score").desc())
    segment_product_stats = segment_product_stats.withColumn("rank", row_number().over(window))
    
    # Top 10 produktów per segment
    top_products = segment_product_stats.filter(col("rank") <= 10)
    
    # JOIN z katalogiem produktów
    logger.info("JOIN z produktami...")
    top_products = top_products.join(products_df, "product_id", "left")
    
    num_segments = top_products.select("age_group").distinct().count()
    logger.info(f"Znaleziono {num_segments} segmentów")
    
    return top_products


def write_to_hbase(recommendations_df):
    """Zapisuje rekomendacje do HBase."""
    logger.info("Zapisuję rekomendacje do HBase...")
    
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT)
        table = connection.table('personalized_recommendations')
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Zbierz wszystkie segmenty
        segments = recommendations_df.select("age_group").distinct().collect()
        
        written = 0
        for segment_row in segments:
            age_group = segment_row['age_group']
            
            # Top 10 produktów dla tego segmentu
            segment_products = recommendations_df.filter(
                col("age_group") == age_group
            ).orderBy("rank").limit(10).collect()
            
            # Row key: timestamp_age_group
            row_key = f"{timestamp}_segment_{age_group}".encode('utf-8')
            
            # Dane do HBase
            hbase_data = {
                b'profile:age_group': age_group.encode('utf-8'),
                b'profile:segment_type': b'demographic',
                b'profile:created_at': datetime.now().isoformat().encode('utf-8')
            }
            
            # Dodaj top 10 produktów
            for product in segment_products:
                rank = product['rank']
                product_id = product['product_id']
                product_name = product['product_name'] if product['product_name'] else 'Unknown'
                score = product['recommendation_score']
                views = product['views']
                
                prefix = f"products:rank_{rank}_".encode('utf-8')
                hbase_data[prefix + b'product_id'] = str(product_id).encode('utf-8')
                hbase_data[prefix + b'product_name'] = product_name.encode('utf-8')
                hbase_data[prefix + b'score'] = str(score).encode('utf-8')
                hbase_data[prefix + b'views'] = str(views).encode('utf-8')
            
            table.put(row_key, hbase_data)
            written += 1
            logger.info(f"Zapisano segment: {age_group}")
        
        connection.close()
        logger.info(f"Łącznie zapisano {written} segmentów do HBase")
        
    except Exception as e:
        logger.error(f"Błąd zapisu do HBase: {e}")
        raise


def main():
    logger.info("=== START Demographic Recommendations ===")
    
    try:
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        # Wczytaj dane
        events_df = load_events(spark)
        users_df = load_users(spark)
        products_df = load_products(spark)
        
        if events_df.count() == 0:
            logger.warning("Brak eventów - kończę")
            return
        
        # Oblicz rekomendacje
        recommendations_df = calculate_recommendations(events_df, users_df, products_df)
        
        # Pokaż przykłady
        logger.info("\n=== Przykładowe rekomendacje ===")
        recommendations_df.select(
            "age_group", "rank", "product_name", "recommendation_score", 
            "views", "add_to_cart", "purchases"
        ).orderBy("age_group", "rank").show(30, truncate=False)
        
        # Zapisz do HBase
        write_to_hbase(recommendations_df)
        
        logger.info("=== KONIEC Demographic Recommendations ===")
        
    except Exception as e:
        logger.error(f"Błąd w main: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

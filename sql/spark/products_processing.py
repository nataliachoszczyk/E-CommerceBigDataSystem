from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("ProductsBatchProcessing") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.table("ecommerce.products_raw")

# Prosta transformacja: filtracja braków i konwersja do Parquet
df = df.filter(col("price").isNotNull() & col("category_code").isNotNull())

df.write.mode("overwrite").partitionBy("category_code").parquet(
    "/processed/products", compression="snappy"
)

spark.stop()

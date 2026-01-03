from pyspark.sql import SparkSession
from pyspark.sql.functions import year, current_date, when, col, count

spark = SparkSession.builder \
    .appName("UsersBatchProcessing") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.table("ecommerce.users_raw")

df = df.withColumn(
    "age",
    year(current_date()) - year(col("birth_date"))
)

df = df.withColumn(
    "age_group",
    when(col("age") < 26, "18-25")
    .when(col("age") < 36, "26-35")
    .when(col("age") < 51, "36-50")
    .otherwise("50+")
)

result = df.groupBy("city", "age_group").agg(
    count("*").alias("users_count")
)

result.write.mode("overwrite").parquet(
    "/processed/user_demographics"
)

spark.stop()

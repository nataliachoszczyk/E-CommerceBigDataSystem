from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("CheckPurchases").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

now = datetime.now()
time_24h_ago = now - timedelta(hours=24)

print(f"\nSprawdzam purchase z ostatnich 24h: {time_24h_ago} -> {now}")

df = spark.sql(f"""
    SELECT event_type, mapped_time 
    FROM user_events 
    WHERE event_type='purchase' 
    AND year={time_24h_ago.year} AND month={time_24h_ago.month} AND day>={time_24h_ago.day}
""")

df = df.withColumn("timestamp", to_timestamp("mapped_time"))
df = df.filter(col("timestamp") >= lit(time_24h_ago))

count = df.count()
print(f"\nLiczba purchase w ostatnich 24h: {count}")

if count > 0:
    df.select(min("timestamp").alias("min"), max("timestamp").alias("max")).show(truncate=False)

spark.stop()

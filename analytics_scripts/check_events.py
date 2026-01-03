from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckEvents").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.sql("SELECT * FROM user_events where year=2025 and month=12 and day=31 LIMIT 100")
df.show()

spark.stop()

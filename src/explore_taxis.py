from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExploreTaxis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("data/yellow_tripdata_2026-01.parquet")

print(f"Row count: {df.count()}")
print(f"Number of partitions: {df.rdd.getNumPartitions()}")
print("DataFrame schema:")
df.printSchema()
print("First rows:")
df.show(5)

spark.stop()
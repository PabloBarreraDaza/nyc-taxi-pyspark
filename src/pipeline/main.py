from extract import get_spark_session, extract_trips

spark = get_spark_session()
df = extract_trips(spark)

print(f"Total rows across all months: {df.count()}")
print(f"Number of partitions: {df.rdd.getNumPartitions()}")

spark.stop()
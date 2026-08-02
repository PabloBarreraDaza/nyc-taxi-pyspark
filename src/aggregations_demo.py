from pyspark.sql.functions import avg, count, sum as spark_sum, round as spark_round
from pathlib import Path
from pyspark.sql import SparkSession


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "yellow_tripdata_2026-01.parquet"

spark = SparkSession.builder.appName("AggregationsDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(str(DATA_PATH))

# Pandas equivalente:
# df.groupby("passenger_count")["fare_amount"].mean()

resultado = df.groupBy("passenger_count") \
    .agg(
        count("*").alias("num_trips"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance")
    ) \
    .orderBy("passenger_count")

resultado.show()
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "yellow_tripdata_2026-01.parquet"

spark = SparkSession.builder.appName("CacheAndWriteDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(str(DATA_PATH))
df_filtrado = df.filter(col("fare_amount") > 0).cache()

start = time.time()
print(f"Count: {df_filtrado.count()}")
print(f"First action (cache not calculated yet): {time.time() - start:.2f}s")

start = time.time()
print(f"Count again: {df_filtrado.count()}")
print(f"Second action (cache): {time.time() - start:.2f}s")

df_with_date = df_filtrado.withColumn("pickup_date", col("tpep_pickup_datetime").cast("date"))
df_with_date.write.partitionBy("pickup_date").mode("overwrite").parquet(
    str(BASE_DIR / "data" / "output" / "trips_by_date")
)
print("Written partitioned Parquet output.")

spark.stop()
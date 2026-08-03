from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "yellow_tripdata_2026-01.parquet"

spark = SparkSession.builder.appName("WindowFunctionsDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(str(DATA_PATH))

window = Window.partitionBy("PULocationID").orderBy(col("fare_amount").desc())

df_top_per_zone = df.withColumn("rank_in_zone", rank().over(window)) \
    .filter(col("rank_in_zone") <= 3) \
    .select("PULocationID", "fare_amount", "trip_distance", "rank_in_zone") \
    .orderBy("PULocationID", "rank_in_zone")

df_top_per_zone.show(15)

spark.stop()
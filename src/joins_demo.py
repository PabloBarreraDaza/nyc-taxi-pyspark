from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

BASE_DIR = Path(__file__).resolve().parent.parent
TAXI_DATA_PATH = BASE_DIR / "data" / "yellow_tripdata_2026-01.parquet"
ZONES_DATA_PATH = BASE_DIR / "data" / "taxi_zone_lookup.csv"

spark = SparkSession.builder.appName("JoinsDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(str(TAXI_DATA_PATH))
df_zones = spark.read.csv(str(ZONES_DATA_PATH), header=True, inferSchema=True)

print("Schema of zones dataset:")
df_zones.printSchema()
df_zones.show(5)

# Join normal
df_with_zone = df.join(
    df_zones,
    df.PULocationID == df_zones.LocationID,
    "left"
)

print("Result of normal join:")
df_with_zone.select("PULocationID", "Zone", "fare_amount").show(5)

# Join con broadcast explícito
df_with_zone_broadcast = df.join(
    broadcast(df_zones),
    df.PULocationID == df_zones.LocationID,
    "left"
)

print("Result of broadcast join:")
df_with_zone_broadcast.select("PULocationID", "Zone", "fare_amount").show(5)

spark.stop()
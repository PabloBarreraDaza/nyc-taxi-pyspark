from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "yellow_tripdata_2026-01.parquet"

spark = SparkSession.builder.appName("TransformationsDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(str(DATA_PATH))

df_reducido = df.select("trip_distance", "fare_amount", "tip_amount", "passenger_count")
# Pandas equivalente:
# df_reducido = df[["trip_distance", "fare_amount", "tip_amount", "passenger_count"]]

df_validos = df_reducido.filter((col("trip_distance") > 0) & (col("fare_amount") > 0))
# Pandas equivalente:
# df_validos = df_reducido[(df_reducido["trip_distance"] > 0) & (df_reducido["fare_amount"] > 0)]

df_con_ratio = df_validos.withColumn("price_per_mile", col("fare_amount") / col("trip_distance"))
# Pandas equivalente:
# df_con_ratio = df_validos.copy()
# df_con_ratio["price_per_mile"] = df_con_ratio["fare_amount"] / df_con_ratio["trip_distance"]

print("Resultado tras select + filter + withColumn:")
df_con_ratio.show(10)
# Pandas equivalente:
# print(df_con_ratio.head(10))

print(f"Filas originales: {df.count()}")
# Pandas equivalente:
# print(f"Filas originales: {len(df)}")
print(f"Filas tras filtrar: {df_con_ratio.count()}")
# Pandas equivalente:
# print(f"Filas tras filtrar: {len(df_con_ratio)}"

spark.stop()
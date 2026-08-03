import time
import pandas as pd
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as spark_round

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "yellow_tripdata_2026-01.parquet"

# --- PANDAS ---
print("=== PANDAS ===")
inicio = time.time()

df_pandas = pd.read_parquet(DATA_PATH)
df_filtrado_pd = df_pandas[df_pandas["fare_amount"] > 0]
resultado_pd = df_filtrado_pd.groupby("PULocationID").agg(
    num_trips=("fare_amount", "count"),
    avg_fare=("fare_amount", "mean")
).round(2)

tiempo_pandas = time.time() - inicio
print(f"Pandas total time: {tiempo_pandas:.2f}s")
print(resultado_pd.head())

# --- SPARK ---
print("\n=== SPARK ===")
spark = SparkSession.builder.appName("BenchmarkSpark").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

inicio = time.time()

df_spark = spark.read.parquet(str(DATA_PATH))
df_filtrado_spark = df_spark.filter(col("fare_amount") > 0)
resultado_spark = df_filtrado_spark.groupBy("PULocationID").agg(
    count("*").alias("num_trips"),
    spark_round(avg("fare_amount"), 2).alias("avg_fare")
)
resultado_spark.show(5)  # acción: fuerza la ejecución real para medir el tiempo correctamente

tiempo_spark = time.time() - inicio
print(f"Spark total time: {tiempo_spark:.2f}s")

spark.stop()

print(f"\n=== COMPARISON ===")
print(f"Pandas: {tiempo_pandas:.2f}s")
print(f"Spark:  {tiempo_spark:.2f}s")
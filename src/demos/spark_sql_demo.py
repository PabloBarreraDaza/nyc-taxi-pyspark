from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "yellow_tripdata_2026-01.parquet"

spark = SparkSession.builder.appName("SparkSQLDemo").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(str(DATA_PATH))
df.createOrReplaceTempView("trips")

resultado = spark.sql("""
    WITH stats_per_zone AS (
        SELECT
            PULocationID,
            COUNT(*) AS num_trips,
            ROUND(AVG(fare_amount), 2) AS avg_fare,
            ROUND(AVG(tip_amount), 2) AS avg_tip
        FROM trips
        WHERE fare_amount > 0
        GROUP BY PULocationID
    )
    SELECT
        *,
        RANK() OVER (ORDER BY avg_tip DESC) AS tip_rank
    FROM stats_per_zone
    WHERE num_trips > 100
    ORDER BY tip_rank
    LIMIT 10
""")

resultado.show()

spark.stop()
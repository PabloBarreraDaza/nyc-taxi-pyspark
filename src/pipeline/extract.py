from pyspark.sql import SparkSession
from config import DATA_DIR, ZONES_DATA_PATH, SPARK_APP_NAME


def get_spark_session():
    spark = SparkSession.builder.appName(SPARK_APP_NAME).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def extract_trips(spark):
    parquet_files = sorted(DATA_DIR.glob("yellow_tripdata_*.parquet"))
    file_paths = [str(f) for f in parquet_files]

    if not file_paths:
        raise FileNotFoundError(f"No parquet files found in {DATA_DIR}")

    print(f"Found {len(file_paths)} monthly files to load")
    return spark.read.parquet(*file_paths)


def extract_zones(spark):
    return spark.read.csv(str(ZONES_DATA_PATH), header=True, inferSchema=True)
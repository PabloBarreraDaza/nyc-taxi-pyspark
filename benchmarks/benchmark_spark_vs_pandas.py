import time
import gc
import statistics
from pathlib import Path
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as spark_round

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

PANDAS_REPETITIONS = 1   # reduced: pandas struggles with 6 months in RAM, repeating is costly
SPARK_REPETITIONS = 3     # Spark handles repeated runs fine

DATASETS = {
    "1 month (~3.7M rows)": ["yellow_tripdata_2026-01.parquet"],
    "3 months (~11M rows)": [
        "yellow_tripdata_2026-01.parquet",
        "yellow_tripdata_2026-02.parquet",
        "yellow_tripdata_2026-03.parquet",
    ],
    "6 months (~23M rows)": sorted(f.name for f in DATA_DIR.glob("yellow_tripdata_*.parquet")),
}


def run_pandas(file_paths):
    dfs = [pd.read_parquet(DATA_DIR / f) for f in file_paths]
    df = pd.concat(dfs, ignore_index=True)
    del dfs  # free the intermediate list as soon as possible

    df_filtered = df[df["fare_amount"] > 0]
    result = df_filtered.groupby("PULocationID").agg(
        num_trips=("fare_amount", "count"),
        avg_fare=("fare_amount", "mean")
    ).round(2)

    del df, df_filtered
    gc.collect()  # force garbage collection before the next repetition
    return result


def run_spark(spark, file_paths):
    full_paths = [str(DATA_DIR / f) for f in file_paths]
    df = spark.read.parquet(*full_paths)

    df_filtered = df.filter(col("fare_amount") > 0)
    result = df_filtered.groupBy("PULocationID").agg(
        count("*").alias("num_trips"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare")
    )
    result.collect()
    return result


def time_it(fn, repetitions, *args):
    times = []
    for i in range(repetitions):
        print(f"    run {i+1}/{repetitions}...")
        start = time.time()
        fn(*args)
        elapsed = time.time() - start
        times.append(elapsed)
        print(f"    -> {elapsed:.2f}s")
    return statistics.median(times), times


def main():
    spark = SparkSession.builder.appName("BenchmarkSparkVsPandas").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    results = []

    for label, files in DATASETS.items():
        print(f"\n=== {label} ===")

        print("  Pandas:")
        try:
            pandas_median, pandas_times = time_it(run_pandas, PANDAS_REPETITIONS, files)
        except MemoryError:
            print("  Pandas FAILED: out of memory")
            pandas_median = None

        print("  Spark:")
        spark_median, spark_times = time_it(run_spark, SPARK_REPETITIONS, spark, files)

        results.append({
            "dataset": label,
            "pandas_median_s": round(pandas_median, 2) if pandas_median else "OOM",
            "spark_median_s": round(spark_median, 2),
        })

    spark.stop()

    print("\n=== SUMMARY ===")
    for r in results:
        print(f"{r['dataset']}: Pandas={r['pandas_median_s']}s | Spark={r['spark_median_s']}s")

    return results


if __name__ == "__main__":
    main()
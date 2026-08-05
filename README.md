# NYC Taxi PySpark Pipeline

A distributed data processing pipeline built with PySpark, analyzing 23+ million NYC Yellow Taxi trip records across 6 months of data — including data quality analysis, business insights, and a rigorous performance benchmark against Pandas.

Built as a hands-on project to learn PySpark fundamentals (distributed computing, lazy evaluation, window functions, joins) while solving a real analytical problem at a scale where Spark's advantages become measurable and honest, not assumed.

## Why this project exists

Most PySpark tutorials use toy datasets that fit comfortably in Pandas, making it impossible to honestly evaluate when Spark is actually worth using. This project deliberately works with a dataset large enough (23M+ rows, ~6 months) to make that comparison meaningful — and documents the results with real numbers, including cases where the expected outcome didn't hold at small scale.

## Dataset

[NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — public, no API key required. Six consecutive months (December 2025 – May 2026), ~23.3 million trip records in Parquet format, plus a small lookup table mapping location IDs to zone names.

## Architecture

```
data/*.parquet (6 monthly files)
        │
        ▼
[extract.py]  Reads all monthly files via Python's pathlib
              (avoids a Windows-specific Hadoop globbing bug)
        │
        ▼
[transform.py]  Derived columns: trip duration, pickup date/hour/weekday,
                 price per mile (safe division via try_divide)
        │
        ▼
[quality.py]  Explicit data quality flags — invalid distances, missing
              passenger counts, fare/distance mismatches, timestamp errors
        │
        ▼
[analysis.py]  5 business questions answered with Spark DataFrame API,
               SQL, and window functions
        │
        ▼
[main.py]  Orchestrates the full pipeline end to end
```

Same layered philosophy as a previous ETL project of mine ([tmdb-etl](https://github.com/PabloBarreraDaza/tmdb-etl-python-sql-project)): each module has a single responsibility, and data quality issues are flagged explicitly rather than silently dropped or hidden.

## Project structure

```
nyc-taxi-pyspark/
├── data/                        # parquet files (gitignored, see setup below)
├── src/
│   ├── demos/                    # exploratory scripts from the learning process
│   │   ├── aggregations_demo.py
│   │   ├── window_functions_demo.py
│   │   ├── joins_demo.py
│   │   ├── spark_sql_demo.py
│   │   └── cache_and_write_demo.py
│   └── pipeline/                  # the actual project
│       ├── config.py
│       ├── extract.py
│       ├── transform.py
│       ├── quality.py
│       ├── analysis.py
│       └── main.py
├── benchmarks/
│   └── benchmark_spark_vs_pandas.py
├── tests/
│   ├── conftest.py
│   ├── test_transform.py
│   ├── test_quality.py
│   └── test_analysis.py
└── requirements.txt
```

## Data quality findings

Applying explicit quality flags across 23.3M records surfaced real, substantial data quality issues in the source data:

| Flag | Count | % of total |
|---|---|---|
| Missing passenger count | 6,007,762 | 25.78% |
| Zero passengers | 83,690 | 0.36% |
| Invalid trip distance (≤ 0) | 729,400 | 3.13% |
| Invalid fare amount (≤ 0) | 179,492 | 0.77% |
| Dropoff before pickup | 7 | 0.00% |
| Fare/distance mismatch | 537,062 | 2.30% |

**Design decision**: flags mark suspicious records without dropping them — the same "flag, don't silently clean" principle applied in my TMDB project. Downstream consumers decide whether to filter based on these flags.

### Outliers caught during business analysis

Early versions of the analysis surfaced clearly invalid results — e.g., a pickup zone showing a 676% average tip, and a "price per mile" of nearly $3,000 in another zone. Investigation traced this to near-zero `fare_amount` or `trip_distance` values that passed the basic `> 0` quality flags but still produced absurd ratios once divided.

This was fixed with dedicated thresholds for ratio-based calculations only (`MIN_FARE_FOR_RATIO = 2.5`, `MIN_DISTANCE_FOR_RATIO = 0.3`), distinct from — and stricter than — the general quality flags. These thresholds don't remove rows from the dataset; they only exclude near-zero denominators from specific ratio calculations in `analysis.py`, where division amplifies small errors into large distortions.

A similar issue appeared in the monthly seasonality analysis: two months showed 1 and 9 trips respectively, clearly corrupted `pickup_datetime` values rather than real data — filtered out via an explicit valid-month whitelist.

## Business questions answered

1. **Peak demand hours** — trip volume by hour of day
2. **Demand by day of week** — Thursday is the busiest day; Monday the quietest
3. **Tip percentage by pickup zone** — which zones tip proportionally more, filtered for statistically meaningful sample sizes and non-distorted ratios
4. **Monthly seasonality** — fare and distance trends across the 6-month window
5. **Top zones by price per mile** — ranked with a Spark window function, same pattern as `RANK() OVER (...)` used in the TMDB project's SQL views

## Spark vs Pandas benchmark

Same aggregation (filter + group by + average) measured against 1, 3, and 6 months of data, run locally on a 16-core machine.

| Dataset size | Pandas (median) | Spark (median, warm) | Speedup |
|---|---|---|---|
| 1 month (~3.7M rows) | 7.94s | 3.85s | ~2x |
| 3 months (~11M rows) | 74.51s | 3.69s | ~20x |
| 6 months (~23.3M rows) | 207.18s | 4.15s | ~50x |

**Key finding**: Spark's execution time stays nearly flat as data volume grows (3.7s → 4.2s across a 6x increase in rows), while Pandas degrades non-linearly — a 6x increase in data led to a ~26x increase in runtime, consistent with memory pressure as the dataset approaches the limits of what comfortably fits in RAM on a single machine.

**Honest caveat**: this comparison runs Spark in local mode on one machine with 16 cores — Pandas is single-threaded, so part of Spark's advantage here comes from exploiting multi-core parallelism rather than true distributed computing across a cluster. At smaller volumes (well under 1M rows), Spark's JVM startup and planning overhead would likely make it slower than Pandas — the crossover point depends on both data volume and available parallelism, not a fixed rule of "Spark is always faster."

Also note: the first Spark run per dataset size is consistently slower than subsequent runs (e.g. 28.8s → 3.85s for the 1-month case) — this reflects one-time JVM and SparkSession startup cost, not the cost of the computation itself.

## Data quality flags vs analysis-level filtering

Two distinct layers of filtering exist in this project, intentionally kept separate:

- **`quality.py`**: general-purpose flags (e.g. `trip_distance <= 0`) applied once, describing the dataset's overall health.
- **`analysis.py`**: stricter, ratio-specific thresholds (e.g. `fare_amount >= 2.5`) applied only where a calculation involves division, since near-zero denominators — technically "valid" by the general flags — still distort ratio-based results.

## Testing

15 unit tests covering `transform.py`, `quality.py`, and `analysis.py`, using small, hand-crafted Spark DataFrames rather than the real dataset — fast, deterministic, and independent of file availability.

A session-scoped `pytest` fixture shares a single `SparkSession` (`master("local[2]")`) across all tests, avoiding the repeated JVM startup cost that a per-test session would incur.

Notably, `test_price_per_mile_is_null_when_distance_is_zero` guards against a real bug found during development: Spark's ANSI mode (enabled by default in Spark 4.x) raises a hard `DIVIDE_BY_ZERO` error instead of silently returning infinity, unlike Pandas. Fixed with `try_divide()`; the test ensures this behavior doesn't silently regress.

```bash
python -m pytest tests/ -v
```

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

Requires Java (JDK 17) installed and on PATH — PySpark runs on the JVM.

### 2. Download the data

Download 6 consecutive months of Yellow Taxi trip data (Parquet format) from the [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), and the [taxi zone lookup table](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv). Place all files in `data/`.

### 3. Run the pipeline

```bash
python src/pipeline/main.py
```

### 4. Run the benchmark

```bash
python benchmarks/benchmark_spark_vs_pandas.py
```

Note: the 6-month Pandas run loads the full dataset into memory and can be slow/memory-intensive on machines with limited RAM.

## Known limitations

- **Windows + Hadoop**: this project avoids installing `winutils.exe` (an unsigned third-party binary required for certain Hadoop filesystem operations on Windows). As a result, writing partitioned Parquet output via `df.write.partitionBy(...)` is implemented in the codebase but was not exercised in this environment — reading data works without it, writing does not. This would not be an issue on Linux/macOS or inside a Docker container.
- **Local-mode benchmark**: performance numbers reflect single-machine, multi-core execution, not a true distributed cluster. Results illustrate Spark's parallelism advantage, not cluster-scale distributed computing.

## Tech stack

- **PySpark** (DataFrame API, Spark SQL, window functions)
- **Pandas** + **PyArrow** (benchmark comparison)
- **pytest** (unit tests, with a session-scoped Spark fixture)

## Related project

[tmdb-etl-python-sql-project](https://github.com/PabloBarreraDaza/tmdb-etl-python-sql-project) — a Python + PostgreSQL ETL pipeline with a bronze/silver/gold architecture, covering the "classic ETL at moderate scale" side of data engineering that complements this project's focus on distributed processing at larger scale.

## Author

Pablo Barrera Daza

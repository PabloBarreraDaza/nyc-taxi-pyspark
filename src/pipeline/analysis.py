from pyspark.sql.functions import col, count, avg, round as spark_round, month
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


# Minimum thresholds to filter out data-entry errors from ratio-based calculations.
# These are more conservative than the quality flags in quality.py, which only
# catch exact zeros -- these thresholds catch near-zero values that still produce
# absurd ratios (e.g. fare_amount=0.50 with tip_amount=5 -> 1000% tip).
MIN_FARE_FOR_RATIO = 2.5
MIN_DISTANCE_FOR_RATIO = 0.3

# Valid month range for this dataset (adjust if you download a different range)
VALID_MONTHS = [12, 1, 2, 3, 4, 5]  # December 2025 through May 2026


def peak_hours(df):
    """Question 1: What are the peak demand hours?"""
    return df.groupBy("pickup_hour") \
        .agg(count("*").alias("num_trips")) \
        .orderBy(col("num_trips").desc())


def demand_by_day_of_week(df):
    """
    Question 2: How does demand vary by day of week?
    Note: Spark's dayofweek() convention -> 1=Sunday, 2=Monday, ..., 7=Saturday
    """
    return df.groupBy("pickup_day_of_week") \
        .agg(count("*").alias("num_trips")) \
        .orderBy("pickup_day_of_week")


def tip_percentage_by_zone(df, min_trips=1000):
    """
    Question 3: Which pickup zones generate the highest tip percentage?
    Filters out near-zero fares to avoid inflated ratios from data-entry errors,
    and requires a minimum sample size per zone.
    """
    df_valid = df.filter(
        (col("flag_invalid_fare") == False) &
        (col("fare_amount") >= MIN_FARE_FOR_RATIO) &
        (col("tip_amount") >= 0)
    )

    return df_valid.withColumn(
        "tip_pct", col("tip_amount") / col("fare_amount") * 100
    ).groupBy("PULocationID") \
        .agg(
            count("*").alias("num_trips"),
            spark_round(avg("tip_pct"), 2).alias("avg_tip_pct")
        ) \
        .filter(col("num_trips") >= min_trips) \
        .orderBy(col("avg_tip_pct").desc())


def monthly_seasonality(df):
    """
    Question 4: Is there seasonality across the 6 months of data?
    Filters out corrupted pickup dates outside the expected data range.
    """
    df_with_month = df.withColumn("trip_month", month(col("pickup_date")))
    df_valid = df_with_month.filter(col("trip_month").isin(VALID_MONTHS))

    return df_valid.groupBy("trip_month") \
        .agg(
            count("*").alias("num_trips"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("trip_distance"), 2).alias("avg_distance")
        ) \
        .orderBy("trip_month")


def top_zones_by_price_per_mile(df, top_n=5, min_trips=500):
    """
    Question 5: Which zones are most profitable per mile traveled?
    Filters out near-zero distances that inflate the price-per-mile ratio.
    """
    df_valid = df.filter(
        (col("flag_invalid_distance") == False) &
        (col("trip_distance") >= MIN_DISTANCE_FOR_RATIO)
    )

    stats = df_valid.groupBy("PULocationID") \
        .agg(
            count("*").alias("num_trips"),
            spark_round(avg("price_per_mile"), 2).alias("avg_price_per_mile")
        ) \
        .filter(col("num_trips") >= min_trips)

    window = Window.orderBy(col("avg_price_per_mile").desc())
    ranked = stats.withColumn("rank", rank().over(window))

    return ranked.filter(col("rank") <= top_n)
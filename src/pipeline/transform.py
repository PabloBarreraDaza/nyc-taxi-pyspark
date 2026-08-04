from pyspark.sql.functions import col, unix_timestamp, to_date, dayofweek, hour, round as spark_round, try_divide


def add_derived_columns(df):
    df = df.withColumn(
        "trip_duration_minutes",
        spark_round(
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60,
            2
        )
    )

    df = df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
    df = df.withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
    df = df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))

    # try_divide returns NULL instead of raising an error when trip_distance = 0,
    # consistent with the "flag, don't silently drop" approach from quality.py
    df = df.withColumn(
        "price_per_mile",
        spark_round(try_divide(col("fare_amount"), col("trip_distance")), 2)
    )

    return df
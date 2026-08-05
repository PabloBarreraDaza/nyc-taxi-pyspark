from analysis import (
    peak_hours,
    demand_by_day_of_week,
    tip_percentage_by_zone,
    monthly_seasonality,
    top_zones_by_price_per_mile,
    MIN_FARE_FOR_RATIO,
)
from quality import add_quality_flags
from transform import add_derived_columns


def sample_full_df(spark):
    """
    Builds a small DataFrame that has already gone through transform + quality,
    same as it would arrive at analysis.py in the real pipeline.
    """
    data = [
        (1, 2.5, 15.0, 3.0, "2026-01-01 18:00:00", "2026-01-01 18:20:00", 100),
        (1, 3.0, 20.0, 4.0, "2026-01-01 18:30:00", "2026-01-01 18:50:00", 100),
        (2, 5.0, 30.0, 6.0, "2026-02-15 08:00:00", "2026-02-15 08:25:00", 200),
        (1, 1.0, 1.0, 5.0, "2026-01-02 09:00:00", "2026-01-02 09:05:00", 300),  # low fare -> excluded from ratio calcs
    ]
    columns = [
        "passenger_count", "trip_distance", "fare_amount", "tip_amount",
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID"
    ]

    df = spark.createDataFrame(data, columns)
    df = df.withColumn("tpep_pickup_datetime", df["tpep_pickup_datetime"].cast("timestamp"))
    df = df.withColumn("tpep_dropoff_datetime", df["tpep_dropoff_datetime"].cast("timestamp"))

    df = add_derived_columns(df)
    df = add_quality_flags(df)
    return df


def test_peak_hours_counts_correctly(spark):
    df = sample_full_df(spark)
    result = peak_hours(df)

    hour_18 = result.filter(result.pickup_hour == 18).collect()[0]
    assert hour_18["num_trips"] == 2


def test_demand_by_day_of_week_sums_to_total(spark):
    df = sample_full_df(spark)
    result = demand_by_day_of_week(df)

    total = sum(row["num_trips"] for row in result.collect())
    assert total == df.count()


def test_tip_percentage_excludes_low_fare_trips(spark):
    """
    The row with fare_amount=1.0 is below MIN_FARE_FOR_RATIO and should
    not distort the tip percentage calculation for its zone.
    """
    df = sample_full_df(spark)
    result = tip_percentage_by_zone(df, min_trips=1)

    zone_300_present = result.filter(result.PULocationID == 300).count()
    assert zone_300_present == 0  # excluded because its only trip has fare_amount=1.0 < MIN_FARE_FOR_RATIO


def test_top_zones_by_price_per_mile_respects_top_n(spark):
    df = sample_full_df(spark)
    result = top_zones_by_price_per_mile(df, top_n=2, min_trips=1)

    assert result.count() <= 2
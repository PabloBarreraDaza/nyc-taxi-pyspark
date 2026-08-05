from transform import add_derived_columns


def sample_raw_df(spark):
    data = [
        (2.5, 15.0, "2026-01-01 10:00:00", "2026-01-01 10:20:00"),  # normal trip, 20 min
        (0.0, 12.0, "2026-01-01 12:00:00", "2026-01-01 12:10:00"),  # zero distance -> price_per_mile should be NULL
        (3.0, 21.0, "2026-01-04 23:30:00", "2026-01-05 00:00:00"),  # crosses midnight, Sunday->Monday
    ]
    columns = ["trip_distance", "fare_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"]

    df = spark.createDataFrame(data, columns)
    df = df.withColumn("tpep_pickup_datetime", df["tpep_pickup_datetime"].cast("timestamp"))
    df = df.withColumn("tpep_dropoff_datetime", df["tpep_dropoff_datetime"].cast("timestamp"))
    return df


def test_add_derived_columns_creates_expected_columns(spark):
    df = sample_raw_df(spark)
    df_transformed = add_derived_columns(df)

    expected_columns = [
        "trip_duration_minutes", "pickup_date", "pickup_day_of_week",
        "pickup_hour", "price_per_mile"
    ]
    for column in expected_columns:
        assert column in df_transformed.columns


def test_trip_duration_calculation(spark):
    df = sample_raw_df(spark)
    df_transformed = add_derived_columns(df)

    first_row = df_transformed.filter(df_transformed.fare_amount == 15.0).collect()[0]
    assert first_row["trip_duration_minutes"] == 20.0


def test_price_per_mile_is_null_when_distance_is_zero(spark):
    df = sample_raw_df(spark)
    df_transformed = add_derived_columns(df)

    zero_distance_row = df_transformed.filter(df_transformed.trip_distance == 0.0).collect()[0]
    assert zero_distance_row["price_per_mile"] is None


def test_price_per_mile_calculation(spark):
    df = sample_raw_df(spark)
    df_transformed = add_derived_columns(df)

    first_row = df_transformed.filter(df_transformed.fare_amount == 15.0).collect()[0]
    # fare_amount=15.0, trip_distance=2.5 -> 15.0 / 2.5 = 6.0
    assert first_row["price_per_mile"] == 6.0


def test_pickup_hour_extraction(spark):
    df = sample_raw_df(spark)
    df_transformed = add_derived_columns(df)

    first_row = df_transformed.filter(df_transformed.fare_amount == 15.0).collect()[0]
    assert first_row["pickup_hour"] == 10
from quality import add_quality_flags, QUALITY_FLAGS


def sample_trips_df(spark):
    data = [
        # (passenger_count, trip_distance, fare_amount, pickup_dt, dropoff_dt)
        (1, 2.5, 15.0, "2026-01-01 10:00:00", "2026-01-01 10:20:00"),  # valid trip
        (None, 3.0, 20.0, "2026-01-01 11:00:00", "2026-01-01 11:15:00"),  # missing passenger_count
        (0, 1.0, 10.0, "2026-01-01 12:00:00", "2026-01-01 12:10:00"),  # zero passengers
        (2, 0.0, 12.0, "2026-01-01 13:00:00", "2026-01-01 13:05:00"),  # invalid distance
        (1, 4.0, 0.0, "2026-01-01 14:00:00", "2026-01-01 14:20:00"),  # invalid fare
        (1, 5.0, 25.0, "2026-01-01 16:00:00", "2026-01-01 15:00:00"),  # dropoff before pickup
    ]
    columns = ["passenger_count", "trip_distance", "fare_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"]

    df = spark.createDataFrame(data, columns)
    df = df.withColumn("tpep_pickup_datetime", df["tpep_pickup_datetime"].cast("timestamp"))
    df = df.withColumn("tpep_dropoff_datetime", df["tpep_dropoff_datetime"].cast("timestamp"))
    return df


def test_add_quality_flags_creates_all_expected_columns(spark):
    df = sample_trips_df(spark)
    df_flagged = add_quality_flags(df)

    for flag in QUALITY_FLAGS:
        assert flag in df_flagged.columns


def test_flag_missing_passenger_count(spark):
    df = sample_trips_df(spark)
    df_flagged = add_quality_flags(df)

    result = df_flagged.filter(df_flagged.flag_missing_passenger_count == True).count()
    assert result == 1  # only the row with passenger_count=None


def test_flag_zero_passengers(spark):
    df = sample_trips_df(spark)
    df_flagged = add_quality_flags(df)

    result = df_flagged.filter(df_flagged.flag_zero_passengers == True).count()
    assert result == 1


def test_flag_invalid_distance(spark):
    df = sample_trips_df(spark)
    df_flagged = add_quality_flags(df)

    result = df_flagged.filter(df_flagged.flag_invalid_distance == True).count()
    assert result == 1


def test_flag_dropoff_before_pickup(spark):
    df = sample_trips_df(spark)
    df_flagged = add_quality_flags(df)

    result = df_flagged.filter(df_flagged.flag_dropoff_before_pickup == True).count()
    assert result == 1


def test_valid_trip_has_no_flags_active(spark):
    """The first row is a fully valid trip -- none of its flags should be True."""
    df = sample_trips_df(spark)
    df_flagged = add_quality_flags(df)

    first_row = df_flagged.filter(df_flagged.fare_amount == 15.0).collect()[0]
    for flag in QUALITY_FLAGS:
        assert first_row[flag] == False
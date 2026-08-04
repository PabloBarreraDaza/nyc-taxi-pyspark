from extract import get_spark_session, extract_trips, extract_zones
from transform import add_derived_columns
from quality import add_quality_flags, quality_summary


def run():
    spark = get_spark_session()

    print("Extracting trip data...")
    df_trips = extract_trips(spark)
    df_zones = extract_zones(spark)
    print(f"Loaded {df_trips.count()} raw trip records")

    print("\nApplying transformations...")
    df_transformed = add_derived_columns(df_trips)

    print("\nApplying quality flags...")
    df_final = add_quality_flags(df_transformed)

    print("\nData quality summary:")
    summary, total = quality_summary(df_final)
    print(f"Total records: {total}")
    for flag_name, count, pct in summary:
        print(f"  {flag_name}: {count} ({pct}%)")

    print("\nSample of final DataFrame:")
    df_final.select(
        "tpep_pickup_datetime",
        "pickup_date",
        "pickup_hour",
        "pickup_day_of_week",
        "trip_duration_minutes",
        "price_per_mile",
        "flag_invalid_distance",
        "flag_zero_passengers"
    ).show(10)

    spark.stop()


if __name__ == "__main__":
    run()
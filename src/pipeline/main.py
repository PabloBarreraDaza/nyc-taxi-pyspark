from extract import get_spark_session, extract_trips, extract_zones
from transform import add_derived_columns
from quality import add_quality_flags, quality_summary
from analysis import peak_hours, demand_by_day_of_week, tip_percentage_by_zone, monthly_seasonality, top_zones_by_price_per_mile


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


    print("\n--- Peak hours ---")
    peak_hours(df_final).show(24)

    print("\n--- Demand by day of week ---")
    demand_by_day_of_week(df_final).show()

    print("\n--- Tip percentage by zone (top 10) ---")
    tip_percentage_by_zone(df_final).show(10)

    print("\n--- Monthly seasonality ---")
    monthly_seasonality(df_final).show()

    print("\n--- Top 5 zones by price per mile ---")
    top_zones_by_price_per_mile(df_final).show()

    spark.stop()


if __name__ == "__main__":
    run()
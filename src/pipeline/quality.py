from pyspark.sql.functions import col, sum as spark_sum


def add_quality_flags(df):
    """
    Adds boolean quality flags to the trips DataFrame.
    Follows the same principle as the TMDB project: never silently drop
    suspicious records, just flag them and let downstream consumers decide.
    """
    df = df.withColumn("flag_missing_passenger_count", col("passenger_count").isNull())
    df = df.withColumn("flag_zero_passengers", col("passenger_count") == 0)
    df = df.withColumn("flag_invalid_distance", col("trip_distance") <= 0)
    df = df.withColumn("flag_invalid_fare", col("fare_amount") <= 0)
    df = df.withColumn(
        "flag_dropoff_before_pickup",
        col("tpep_dropoff_datetime") < col("tpep_pickup_datetime")
    )
    df = df.withColumn(
        "flag_fare_distance_mismatch",
        ((col("trip_distance") > 1) & (col("fare_amount") <= 0)) |
        ((col("trip_distance") <= 0) & (col("fare_amount") > 20))
    )
    return df


QUALITY_FLAGS = [
    "flag_missing_passenger_count",
    "flag_zero_passengers",
    "flag_invalid_distance",
    "flag_invalid_fare",
    "flag_dropoff_before_pickup",
    "flag_fare_distance_mismatch",
]


def quality_summary(df):
    """
    Returns a list of (flag_name, count, percentage) tuples,
    equivalent to gold.data_quality_report in the TMDB project.
    """
    total = df.count()

    """
    Cada flag es True/False (booleano). Al hacer .cast("int"), True se convierte en 1 y False en 0. 
    Si tienes una columna de 23 millones de 0s y 1s, y le haces spark_sum(...), 
    el resultado es literalmente cuántos 1 había — es decir, cuántas
    filas tenían ese flag en True. Es un truco muy común: "sumar una condición booleana" = "contar cuántas veces se cumple".

    
    df.agg(*exprs): ejecuta las 6 agregaciones (sumas) a la vez, en una sola pasada por los datos 
    — el * aquí desempaqueta la lista exprs en argumentos separados de la función .agg()
    [0]: como collect() devuelve una lista (aunque aquí solo tenga 1 elemento),
      cogemos el primer (y único) elemento — esa única fila con los 6 totales
    """
    exprs = [spark_sum(col(f).cast("int")).alias(f) for f in QUALITY_FLAGS]
    counts_row = df.agg(*exprs).collect()[0]

    summary = []
    for flag in QUALITY_FLAGS:
        count = counts_row[flag]
        pct = round(100.0 * count / total, 2) if total > 0 else 0
        summary.append((flag, count, pct))

    return summary, total
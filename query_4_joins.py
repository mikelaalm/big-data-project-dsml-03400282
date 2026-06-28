# QUERY 4 - JOIN STRATEGY COMPARISON (Requirement 6)

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, udf, round as spark_round, row_number, count, avg
from pyspark.sql.types import DoubleType
import time
import math

HDFS    = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
CRIME_2010 = f"{HDFS}/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020 = f"{HDFS}/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
STATIONS   = f"{HDFS}/data/LA_Police_Stations.csv"

spark = (
    SparkSession.builder
    .appName("Query4_JoinStrategies")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("QUERY 4 - JOIN STRATEGY COMPARISON")
print("=" * 60)


# Haversine UDF
def haversine_distance(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None
    R = 6371
    lat1_rad, lon1_rad = math.radians(lat1), math.radians(lon1)
    lat2_rad, lon2_rad = math.radians(lat2), math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

haversine_udf = udf(haversine_distance, DoubleType())


# Load data once

print("\n[SETUP] Loading data...")

crime_df = (
    spark.read.csv(CRIME_2010, header=True, inferSchema=True)
    .union(spark.read.csv(CRIME_2020, header=True, inferSchema=True))
    .select(
        col("LAT").cast("double").alias("crime_lat"),
        col("LON").cast("double").alias("crime_lon")
    )
    .filter(col("crime_lat").isNotNull() & col("crime_lon").isNotNull())
    .cache()
)

stations_df = (
    spark.read.csv(STATIONS, header=True, inferSchema=True)
    .select(
        col("DIVISION").alias("division"),
        col("X").cast("double").alias("station_lon"),
        col("Y").cast("double").alias("station_lat")
    )
    .filter(col("station_lat").isNotNull() & col("station_lon").isNotNull())
    .cache()
)

crime_count   = crime_df.count()
station_count = stations_df.count()
print(f"  - Crimes: {crime_count:,}")
print(f"  - Stations: {station_count}")


# run query with a given join hint
def run_with_hint(hint_name):
    print(f"\n{'=' * 60}")
    print(f"JOIN STRATEGY: {hint_name}")
    print('=' * 60)

    start = time.time()

    if hint_name == "DEFAULT":
        crimes_stations = crime_df.crossJoin(stations_df)
    else:
        crimes_stations = crime_df.crossJoin(stations_df.hint(hint_name))

    crimes_stations = crimes_stations.withColumn(
        "distance",
        haversine_udf(
            col("crime_lat"), col("crime_lon"),
            col("station_lat"), col("station_lon")
        )
    )

    window_spec = Window.partitionBy("crime_lat", "crime_lon").orderBy(col("distance").asc())

    crimes_with_nearest = crimes_stations.withColumn(
        "rank", row_number().over(window_spec)
    ).filter(col("rank") == 1).drop("rank")

    result = crimes_with_nearest.groupBy("division").agg(
        count("*").alias("crime_count"),
        spark_round(avg("distance"), 3).alias("average_distance")
    ).orderBy(col("crime_count").desc())

    print("\n-- Physical Plan --")
    result.explain(extended=False)

    result.show(5, truncate=False)
    result.count()

    elapsed = time.time() - start
    print(f"\nExecution time: {elapsed:.2f} seconds")
    return elapsed


# Run all strategies
strategies = ["DEFAULT", "broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]
timings = {}

for strategy in strategies:
    timings[strategy] = run_with_hint(strategy)


# Summary
print("\n" + "=" * 60)
print("SUMMARY - Query 4 Join Strategy Comparison")
print("=" * 60)
print(f"{'Strategy':<25} {'Time (s)':>10}")
print("-" * 36)
for strategy, t in timings.items():
    print(f"{strategy:<25} {t:>10.2f}")

best = min(timings, key=timings.get)
print(f"\nFastest strategy: {best} ({timings[best]:.2f}s)")
print("=" * 60 + "\n")

spark.stop()

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import(
    col, udf, round as spark_round, row_number,
    count, avg, desc, radians, sin, cos, sqrt, atan2
)
from pyspark.sql.types import DoubleType
import time
import math

HDFS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
CRIME_2010 = f"{HDFS}/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020 = f"{HDFS}/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
STATIONS = f"{HDFS}/data/LA_Police_Stations.csv"

spark = (
    SparkSession.builder
    .appName("Query4_CrimesByNearestStation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("QUERY 4 - Crimes by Nearest Police Station")
print("\n" + "=" * 60)

start_time = time.time()

print("\n[1/5] Loading Data...")
crime_df = (
    spark.read.csv(CRIME_2010, header = True, inferSchema = True)
    .union(spark.read.csv(CRIME_2020, header = True, inferSchema = True))
    .select(
        col("LAT").cast("double").alias("crime_lat"),
        col("LON").cast("double").alias("crime_lon")
    )
    .filter(col("crime_lat").isNotNull() & col("crime_lon").isNotNull())
)

stations_df = (
    spark.read.csv(STATIONS, header = True, inferSchema = True)
    .select(
        col("DIVISION").alias("division"),
        col("X").cast("double").alias("station_lon"),
        col("Y").cast("double").alias("station_lat")
    )
    .filter(col("station_lat").isNotNull() & col("station_lon").isNotNull())
)

crime_count = crime_df.count()
station_count = stations_df.count()
print(f" - Loaded {crime_count:,} crimes")
print(f" - Loaded {station_count} police stations")


def haversine_distance(lat1, lon1, lat2, lon2):
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    R = 6371 # earth radius (km)

    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

    distance = R * c
    return distance

haversine_udf = udf(haversine_distance, DoubleType())

print("\n[2/5] Calculating distances (Cross join with all stations)...")

crimes_stations = crime_df.crossJoin(stations_df)

crimes_stations = crimes_stations.withColumn(
    "distance",
    haversine_udf(
        col("crime_lat"),
        col("crime_lon"),
        col("station_lat"),
        col("station_lon")
    )
)

window_spec = Window.partitionBy("crime_lat", "crime_lon").orderBy(col("distance").asc())

crimes_with_nearest = crimes_stations.withColumn(
    "rank",
    row_number().over(window_spec)
).filter(col("rank") == 1).drop("rank")

print(f" - Computed distance calculations")


print("\n[3/5] Aggregating results by station...")

# print(f"\n[DEBUG] Distinct divisions in crimes_with_nearest: {crimes_with_nearest.select('division').distinct().count()}")
# print("[DEBUG] Sample of crimes_with_nearest:")
# crimes_with_nearest.select("division", "distance").distinct().show(25, truncate=False)


result_df = crimes_with_nearest.groupBy("division").agg(
    count("*").alias("crime_count"),
    spark_round(avg("distance"), 3).alias("average_distance")
).orderBy(col("crime_count").desc())

print(f" -Results ready")

print("\n [4/5] Query Execution Plan:")
print("=" * 60)
result_df.explain(extended = False)

print("\n[5/5] Results:")
print("=" * 60)
result_df.show(n = 30, truncate = False)

row_count = result_df.count()

end_time = time.time()
exec_time = end_time - start_time

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Total stations: {row_count}")
print(f"Total execution time: {exec_time:.2f} seconds")
print(f"Data has been processed: {crime_count:,} crimes x {station_count} stations")
print("=" * 60 + "\n")

spark.stop()

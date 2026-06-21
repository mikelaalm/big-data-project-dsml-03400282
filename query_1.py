import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

# HDFS paths
HDFS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
CRIME_2010 = f"{HDFS}/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020 = f"{HDFS}/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"

# Spark session
spark = (
    SparkSession.builder
    .appName("Query1_StreetCrimeByTimeOfDay")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Load and cache data
# Read both CSV, union them, keep only the 2 columns needed
# Cast TIME OCC to int to be safe (in an be read as a string by inferSchema)
print("Loading data:")
crime_df = (
    spark.read.csv(CRIME_2010, header = True, inferSchema = True)
    .union(spark.read.csv(CRIME_2020, header = True, inferSchema = True))
    .select(
        F.col("TIME OCC").cast("int").alias("time_occ"),
        F.col("Premis Desc").alias("premis_desc"),
    )
    .cache()
)

total_rows = crime_df.count()
print(f"Total records loaded: {total_rows:,}\n")

# Shared helper
def _classify(t: int):
    """Classify a TIME OCC integer into a time-of-day string"""
    if t is None:
        return None
    if 500 <= t <= 1159:
        return "Morning"
    if 1200 <= t <= 1659:
        return "Afternoon"
    if 1700 <= t <= 2059:
        return "Evening"
    return "Night" # covers 2100-2359 and 0-459


# Timing helper
timings = {}

def timed_show(label: str, df_or_rdd, is_rdd = False):
    t0 = time.time()
    if is_rdd:
        rows = df_or_rdd.collect()
        elapsed = time.time() - t0
        print(f"\n{'Time of Day':<12} {'Street %':>10}")
        print("-" * 26)
        for name, pct in rows:
            print(f"{name:<12} {pct:>9.2f}%")
    else:
        df_or_rdd.show(truncate = False)
        elapsed = time.time() - t0
    timings[label] = elapsed
    print(f"---> Execution time [{label}]: {elapsed:.2f}s\n")


# Implementation 1: Dataframe API, no UDF
print("=" * 60)
print("Implementation 1 - Dataframe API (no UDF)")
print("=" * 60)

tod_expr = (
    F.when((F.col("time_occ") >= 500) & (F.col("time_occ") <= 1159), "Morning")
    .when((F.col("time_occ") >= 1200) & (F.col("time_occ") <= 1659), "Afternoon")
    .when((F.col("time_occ") >= 1700) & (F.col("time_occ") <= 2059), "Evening")
    .otherwise("Night")
)

result1 = (
    crime_df.
    .withColumn("time_of_day", tod_expr)
    .groupBy("time_of_day")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("premis_desc") == "STREET", 1).otherwise(0)).alias("street"),
        
    )
    .withColumn("street_pct", F.round(F.col("street") / F.col("total") * 100, 2))
    .select("time_of_day", "street_pct")
    .orderBy(F.col("street_pct").desc())
)

timed_show("Dataframe (no UDF)", result1)
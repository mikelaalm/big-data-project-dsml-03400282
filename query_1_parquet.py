# QUERY 1 - CSV vs PARQUET PERFORMANCE COMPARISON

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
CRIME_2010_CSV = f"{HDFS}/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020_CSV = f"{HDFS}/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
PARQUET_PATH = f"{HDFS}/user/dsml00282/LA_Crime_Data_parquet"

spark = (
    SparkSession.builder
    .appName("Query1_CSV_vs_Parquet")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

tod_expr = (
    F.when((F.col("time_occ") >= 500)  & (F.col("time_occ") <= 1159), "Morning")
    .when((F.col("time_occ") >= 1200) & (F.col("time_occ") <= 1659), "Afternoon")
    .when((F.col("time_occ") >= 1700) & (F.col("time_occ") <= 2059), "Evening")
    .otherwise("Night")
)


def run_query1(df):
    
    return (
        df
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


# RUN 1: CSV FORMAT
print("\n" + "=" * 60)
print("QUERY 1 - CSV Format")
print("=" * 60)

start_csv = time.time()

crime_csv = (
    spark.read.csv(CRIME_2010_CSV, header=True, inferSchema=True)
    .union(spark.read.csv(CRIME_2020_CSV, header=True, inferSchema=True))
    .select(
        F.col("TIME OCC").cast("int").alias("time_occ"),
        F.col("Premis Desc").alias("premis_desc")
    )
)

run_query1(crime_csv).show(truncate=False)
csv_time = time.time() - start_csv
print(f"CSV Execution Time: {csv_time:.2f} seconds")


# RUN 2: PARQUET FORMAT
print("\n" + "=" * 60)
print("QUERY 1 - Parquet Format")
print("=" * 60)

start_parquet = time.time()

crime_parquet = (
    spark.read.parquet(PARQUET_PATH)
    .select(
        F.col("TIME OCC").cast("int").alias("time_occ"),
        F.col("Premis Desc").alias("premis_desc")
    )
)

run_query1(crime_parquet).show(truncate=False)
parquet_time = time.time() - start_parquet
print(f"Parquet Execution Time: {parquet_time:.2f} seconds")


# SUMMARY
print("\n" + "=" * 60)
print("SUMMARY - CSV vs Parquet")
print("=" * 60)
print(f"CSV execution time:     {csv_time:.2f} seconds")
print(f"Parquet execution time: {parquet_time:.2f} seconds")

if parquet_time < csv_time:
    speedup = csv_time / parquet_time
    diff_pct = (csv_time - parquet_time) / csv_time * 100
    print(f"Parquet is {speedup:.2f}x faster ({diff_pct:.1f}% improvement)")
else:
    speedup = parquet_time / csv_time
    diff_pct = (parquet_time - csv_time) / parquet_time * 100
    print(f"CSV is {speedup:.2f}x faster ({diff_pct:.1f}% improvement)")
print("=" * 60 + "\n")

spark.stop()
# CONVERT CRIME CSV DATA TO PARQUET FORMAT

from pyspark.sql import SparkSession
import time

HDFS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
CRIME_2010_CSV = f"{HDFS}/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
CRIME_2020_CSV = f"{HDFS}/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
PARQUET_OUTPUT = f"{HDFS}/user/dsml00282/LA_Crime_Data_parquet"

spark = (
    SparkSession.builder
    .appName("ConvertCrimeCSVToParquet")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("CONVERTING CRIME CSV DATA TO PARQUET")
print("=" * 60)

start_time = time.time()

print("\n[1/3] Reading CSV files...")
crime_df = (
    spark.read.csv(CRIME_2010_CSV, header=True, inferSchema=True)
    .union(spark.read.csv(CRIME_2020_CSV, header=True, inferSchema=True))
)

row_count = crime_df.count()
col_count = len(crime_df.columns)
print(f"  - Loaded {row_count:,} rows, {col_count} columns")

print(f"\n[2/3] Writing to Parquet at:")
print(f"  {PARQUET_OUTPUT}")
crime_df.write.mode("overwrite").parquet(PARQUET_OUTPUT)

end_time = time.time()
exec_time = end_time - start_time

print(f"\n[3/3] Done!")
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Rows converted: {row_count:,}")
print(f"Output path: {PARQUET_OUTPUT}")
print(f"Total conversion time: {exec_time:.2f} seconds")
print("=" * 60 + "\n")

spark.stop()
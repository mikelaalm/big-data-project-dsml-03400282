#!/usr/bin/env python3
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ExploreCrimeData").master("local[2]").getOrCreate()

print("\n=== CRIME DATA (2010-2019) ===")
df = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
)
print("\nSCHEMA:")
df.printSchema()
print("\nFIRST 3 ROWS:")
df.show(3, truncate=False)
print(f"\nCOLUMNS: {df.columns}")

print("\n=== CRIME DATA (2020-2025) ===")
df2 = spark.read.option("header", "true").option("inferSchema", "true").csv(
    "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"
)
print("\nSCHEMA:")
df2.printSchema()

spark.stop()

# -------
# QUERY 2
# -------

import time
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, year, month, count, desc, rank, to_date
from pyspark.sql.functions import substring, when

# Configuration

HDFS_CRIME_2010_2019 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv"
HDFS_CRIME_2020_2025 = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv"

# Helper functions


def load_crime_data(spark):
    df_2010_2019 = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_CRIME_2010_2019)
    df_2020_2025 = spark.read.option("header", "true").option("inferSchema", "true").csv(HDFS_CRIME_2020_2025)
    
    combined_df = df_2010_2019.union(df_2020_2025)
    
    # Extract year (positions 0-3) and month name (positions 5-7)
    parsed_df = combined_df.withColumn(
        "crime_year",
        substring(col("Date Rptd"), 0, 4).cast("int")  # "2010"
    ).withColumn(
        "month_name",
        substring(col("Date Rptd"), 6, 3)  # "Feb"
    ).withColumn(
        "crime_month",
        when(col("month_name") == "Jan", 1)
        .when(col("month_name") == "Feb", 2)
        .when(col("month_name") == "Mar", 3)
        .when(col("month_name") == "Apr", 4)
        .when(col("month_name") == "May", 5)
        .when(col("month_name") == "Jun", 6)
        .when(col("month_name") == "Jul", 7)
        .when(col("month_name") == "Aug", 8)
        .when(col("month_name") == "Sep", 9)
        .when(col("month_name") == "Oct", 10)
        .when(col("month_name") == "Nov", 11)
        .when(col("month_name") == "Dec", 12)
    )
    
    return parsed_df


# Implementation 1: DataFrame API

def query_2_dataframe(spark, crime_df):
    
    from pyspark.sql.functions import desc, rank as spark_rank

    crimes_per_month = crime_df.groupBy(
        col("crime_year").alias("year"),
        col("crime_month").alias("month")
    ).count().withColumnRenamed("count", "crime_total")

    window_spec = Window.partitionBy("year").orderBy(desc("crime_total"))
    ranked_months = crimes_per_month.withColumn(
        "ranking",
        spark_rank().over(window_spec)
    )

    top_3_per_year = ranked_months.filter(col("ranking") <= 3)

    result = top_3_per_year.orderBy(
        col("year").asc(), 
        col("crime_total").desc(),
        col("ranking").asc()
    )

    return result

# Implementation 2: SQL API

def query_2_sql(spark, crime_df):
    crime_df.createOrReplaceTempView("crime_data")

    query = """
    WITH crimes_per_month AS (
        SELECT
            crime_year AS year,
            crime_month AS month, 
            COUNT(*) AS crime_total
        FROM crime_data
        GROUP BY crime_year, crime_month
    ),
    ranked_months AS (
        SELECT
            year,
            month,
            crime_total,
            RANK() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
        FROM crimes_per_month
    )

    SELECT
        year,
        month, 
        crime_total,
        ranking
    FROM ranked_months
    WHERE ranking <= 3
    ORDER BY year ASC, crime_total DESC, ranking ASC
    """

    result = spark.sql(query)
    return result


# Main execution

def main():
    spark = SparkSession.builder.appName("Query2_TopMonthsByCrimeCount").getOrCreate()

    print("\n" + "=" * 60)
    print("QUERY 2: Top 3 Months by Crime Count (per Year)")
    print("=" * 60)

    print("\n[1/4] Loading crime data from HDFS...")
    crime_df = load_crime_data(spark)
    print(f" - Loaded {crime_df.count()} crime_records")

    # Implementation 1: DataFrame API
    print("\n[2/4] Executing Query 2 using DataFrame API...")
    start_df = time.time()
    result_df = query_2_dataframe(spark, crime_df)
    result_df.persist() 
    result_count_df = result_df.count()
    time_df = time.time() - start_df

    print(f" - Completed in {time_df:.2f} s")
    print(f" - Result rows: {result_count_df}")

    print("\n First 10 rows (DataFrame API):")
    result_df.show(10, truncate = False)

    # Implementation 2: SQL API
    print("\n[3/4] Executing Query 2 using SQL API...")
    start_sql = time.time()
    result_sql = query_2_sql(spark, crime_df)
    result_sql.persist()
    result_count_sql = result_sql.count()
    time_sql = time.time() - start_sql

    print(f" - Completed in {time_sql:.2f}s")
    print(f" - Result rows: {result_count_sql}")
    
    print("\n First 10 rows (SQL API):")
    result_sql.show(10, truncate = False)

    # Comparison
    print("\n [4/4] Performance Comparison")
    print("=" * 60)
    print(f"DataFrame API execution time: {time_df:.2f} seconds")
    print(f"SQL API execution time: {time_sql:.2f} seconds")

    time_diff = abs(time_df - time_sql)
    time_diff_pct = (time_diff / min(time_df, time_sql)) * 100

    if time_df < time_sql:
        print(f"\n DataFrame API is {time_diff_pct:.1f}% faster")
    elif time_sql < time_df:
        print(f"\n SQL API is {time_diff_pct:.1f}% faster")
    else:
        print(f"\n Both Implementations have similar performance")
    
    print("\nVerifying result consistency...")
    diff_count = result_df.subtract(result_sql).count()
    if diff_count == 0:
        print("Both implementations produce identical results.")
    else:
        print(f"Warning: {diff_count} rows differ between implementations.")

    print("\n" + "=" * 60)
    print("Query 2 execution completed.")
    print("\n" + "=" * 60)

    spark.stop()
    return result_df, result_sql, time_df, time_sql

if __name__ == "__main__":
    main()

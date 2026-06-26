# QUERY 3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import time
import json
import sys

# Implementation 1: DataFrame API

def query_3_dataframe(spark):
    print("\n" + "=" * 60)
    print("QUERY 3 - DataFrame API Implementation")
    print("\n" + "=" * 60)

    start_time = time.time()

    geojson_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Census_Blocks_2020.geojson"
    
    raw_geojson = spark.read.text(geojson_path)
    geojson_text = raw_geojson.rdd.map(lambda row: row[0]).collect()
    geojson_str = "".join(geojson_text)
    geojson_data = json.loads(geojson_str)

    features = []
    for feature in geojson_data['features']:
        props = feature['properties']
        zcta = props.get('ZCTA20')
        pop = props.get('POP20')
        if zcta and pop:
            features.append({'zipcode': str(zcta), 'population': int(pop)})

    census_df = spark.createDataFrame(features)

    census_agg = census_df.groupBy('zipcode').agg(
        spark_sum('population').alias('total_population')
    )

    income_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_income_2021.csv"

    income_df = spark.read.option("delimiter", ";").option("header", "true").csv(income_path)

    income_df = income_df.select(
        col(income_df.columns[0]).alias('zipcode'),
        col(income_df.columns[2]).alias('median_income')
    )

    income_df = income_df.filter(col('zipcode').isNotNull() & col('median_income').isNotNull())

    result_df = census_agg.join(
        income_df,
        on = 'zipcode',
        how = 'inner'
    )

    result_df = result_df.selectExpr(
        'zipcode',
        'total_population',
        'median_income',
        '(CAST(median_income AS DOUBLE) / total_population) AS per_capita_income_2020_2021'
    ).orderBy('zipcode')

    result_df.show(100)
    row_count = result_df.count()

    end_time = time.time()
    exec_time = end_time - start_time

    print(f"\nTotal rows: {row_count}")
    print(f"Execution Time: {exec_time:.2f} seconds")

    return exec_time


# Implementation 2: RDD API

def query_3_rdd(spark):
    print("\n" + "=" * 60)
    print("QUERY 3 - RDD API Implementation")
    print("\n" + "=" * 60)

    sc = spark.sparkContext
    start_time = time.time()

    geojson_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Census_Blocks_2020.geojson"
    
    raw_geojson = sc.textFile(geojson_path)
    geojson_text = raw_geojson.collect()
    geojson_str = "".join(geojson_text)
    geojson_data = json.loads(geojson_str)

    census_rdd = sc.parallelize([
        (feature['properties'].get('ZCTA20'), feature['properties'].get('POP20'))
        for feature in geojson_data['features']
        if feature['properties'].get('ZCTA20') and feature['properties'].get('POP20')
    ])

    census_aggregated = census_rdd.map(lambda x: (str(x[0]), int(x[1]))).reduceByKey(lambda a, b: a + b)

    income_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_income_2021.csv"

    income_rdd = sc.textFile(income_path).filter(lambda x: x.strip())
    income_data = (
        income_rdd
        .zipWithIndex()
        .filter(lambda x: x[1] > 0)
        .map(lambda x: x[0].split(';'))
        .filter(lambda x: len(x) >= 3)
        .filter(lambda x: x[2].strip() not in ['---', ''] and x[2].strip().replace('$', '').replace(',', '').replace('.', '', 1).isdigit())
        .map(lambda x: (str(x[0].strip()), float(x[2].strip().replace('$', '').replace(',',''))))
    )

    result_rdd = census_aggregated.join(income_data)

    per_capita_rdd = result_rdd.map(lambda x: (
        x[0], 
        x[1][0],
        x[1][1],
        x[1][1] / x[1][0]
        )).sortBy(lambda x: x[0])
        
    results = per_capita_rdd.collect()

    print(f"\n{'ZIP Code':<12} {'Population':<15} {'Median Income':<18} {'Per Capita Income':<20}")
    print("=" * 60)
    for zipcode, pop, income, per_capita in results[:100]:
        print(f"{zipcode:<12} {pop:<15} {income:<18.2f} {per_capita:<20.2f}")

    end_time = time.time()
    exec_time = end_time - start_time

    print(f"\nTotal rows: {len(results)}")
    print(f"Execution Time: {exec_time:.2f} seconds")

    return exec_time

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Query3_PerCapitaIncome").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    df_time = query_3_dataframe(spark)
    rdd_time = query_3_rdd(spark)
    
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"DataFrame API: {df_time:.2f} seconds")
    print(f"RDD API: {rdd_time:.2f} seconds")
    print(f"Difference: {abs(df_time - rdd_time):.2f} seconds ({100 * abs(df_time - rdd_time) / max(df_time, rdd_time):.1f}%)")
    
    if df_time < rdd_time:
        print(f"→ DataFrame is {rdd_time / df_time:.2f}x faster")
    else:
        print(f"→ RDD is {df_time / rdd_time:.2f}x faster")
    
    print()
    spark.stop()
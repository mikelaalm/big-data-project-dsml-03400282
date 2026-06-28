# QUERY 3 - JOIN STRATEGY COMPARISON (Requirement 6)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, regexp_replace
import time
import json

HDFS = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
GEOJSON_PATH = f"{HDFS}/data/LA_Census_Blocks_2020.geojson"
INCOME_PATH  = f"{HDFS}/data/LA_income_2021.csv"

spark = (
    SparkSession.builder
    .appName("Query3_JoinStrategies")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "=" * 60)
print("QUERY 3 - JOIN STRATEGY COMPARISON")
print("=" * 60)


# Load data once

print("\n[SETUP] Loading data...")

raw_geojson = spark.read.text(GEOJSON_PATH)
geojson_text = raw_geojson.rdd.map(lambda row: row[0]).collect()
geojson_str  = "".join(geojson_text)
geojson_data = json.loads(geojson_str)

features = []
for feature in geojson_data['features']:
    props = feature['properties']
    zcta  = props.get('ZCTA20')
    pop   = props.get('POP20')
    if zcta and pop:
        features.append({'zipcode': str(zcta), 'population': int(pop)})

census_df = spark.createDataFrame(features)
census_agg = census_df.groupBy('zipcode').agg(
    spark_sum('population').alias('total_population')
).cache()

income_df = (
    spark.read.option("delimiter", ";").option("header", "true").csv(INCOME_PATH)
    .select(
        col("Zip Code").alias('zipcode'),
        regexp_replace(col("Estimated Median Income"), r'[\$,]', '').alias('median_income')
    )
    .filter(col('zipcode').isNotNull() & col('median_income').isNotNull())
    .cache()
)

# Force cache
census_agg.count()
income_df.count()
print(f"  - Census ZIP codes: {census_agg.count()}")
print(f"  - Income ZIP codes: {income_df.count()}")


# Run query with a given join hint

def run_with_hint(hint_name):
    print(f"\n{'=' * 60}")
    print(f"JOIN STRATEGY: {hint_name}")
    print('=' * 60)

    start = time.time()

    if hint_name == "DEFAULT":
        joined = census_agg.join(income_df, on='zipcode', how='inner')
    else:
        joined = census_agg.join(
            income_df.hint(hint_name),
            on='zipcode',
            how='inner'
        )

    result = joined.selectExpr(
        'zipcode',
        'total_population',
        'median_income',
        'ROUND((CAST(median_income AS DOUBLE) / total_population), 2) AS per_capita_income_2020_2021'
    ).orderBy('zipcode')

    print("\n-- Physical Plan --")
    result.explain(extended=False)

    result.show(5, truncate=False)
    count = result.count()

    elapsed = time.time() - start
    print(f"\nRows: {count} | Execution time: {elapsed:.2f} seconds")
    return elapsed


# Run all strategies
strategies = ["DEFAULT", "broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]
timings = {}

for strategy in strategies:
    timings[strategy] = run_with_hint(strategy)


# Summary
print("\n" + "=" * 60)
print("SUMMARY - Query 3 Join Strategy Comparison")
print("=" * 60)
print(f"{'Strategy':<25} {'Time (s)':>10}")
print("-" * 36)
for strategy, t in timings.items():
    print(f"{strategy:<25} {t:>10.2f}")

best = min(timings, key=timings.get)
print(f"\nFastest strategy: {best} ({timings[best]:.2f}s)")
print("=" * 60 + "\n")

spark.stop()

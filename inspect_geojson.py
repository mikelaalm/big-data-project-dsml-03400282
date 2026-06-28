#!/usr/bin/env python3
"""
Inspect the structure of the LA Census Blocks GeoJSON file
to understand available fields for Query 3.
"""

from pyspark.sql import SparkSession
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("InspectGeoJSON") \
    .getOrCreate()

# Read the GeoJSON file
geojson_path = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/LA_Census_Blocks_2020.geojson"

try:
    # Read the file as text
    raw_data = spark.read.text(geojson_path)
    
    # Collect first few lines to inspect
    print("=" * 80)
    print("First 5 lines of GeoJSON file:")
    print("=" * 80)
    lines = raw_data.collect()[:5]
    for i, line in enumerate(lines):
        print(f"Line {i}: {line[0][:200]}")  # Print first 200 chars
    
    print("\n" + "=" * 80)
    print("Full first record for structure analysis:")
    print("=" * 80)
    
    # Try to parse the full content as FeatureCollection
    full_content = raw_data.rdd.map(lambda row: row[0]).collect()
    full_text = "".join(full_content)
    
    # Parse JSON
    geojson = json.loads(full_text)
    
    # Print structure info
    print(f"Type: {geojson.get('type')}")
    print(f"Number of features: {len(geojson.get('features', []))}")
    
    if geojson.get('features'):
        first_feature = geojson['features'][0]
        print("\nFirst feature structure:")
        print(f"  Type: {first_feature.get('type')}")
        print(f"  Geometry type: {first_feature.get('geometry', {}).get('type')}")
        print(f"\n  Properties (first feature):")
        properties = first_feature.get('properties', {})
        for key, value in sorted(properties.items()):
            print(f"    {key}: {value} (type: {type(value).__name__})")
    
except Exception as e:
    print(f"Error reading/parsing GeoJSON: {e}")
    import traceback
    traceback.print_exc()

spark.stop()

# Big Data Management - Semester Project
**NTUA - DSML 2025-26**

## Description
Analysis of Los Angeles crime data using Apache Spark and Hadoop on a Kubernetes cluster.

## Datasets (on HDFS)
- LA Crime Data 2010-2019
- LA Crime Data 2020-2025
- LA Census Blocks 2020
- Median Household Income by Zip Code
- LA Police Stations

## Queries
- Query 1: Crime distribution by time of day at STREET locations
- Query 2: Top 3 months per year by crime count
- Query 3: Per-capita income per ZIP code (2020-2021)
- Query 4: Crimes per police division + average distance

## Environment
- Apache Spark 3.5.8
- Apache Hadoop 3.3.6
- Kubernetes cluster (NTUA cslab)
- Python (PySpark)

## How to run
Scripts are submitted via spark-submit to the Kubernetes cluster.

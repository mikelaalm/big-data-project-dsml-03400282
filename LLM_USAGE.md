# LLM Usage Declaration
## Large-Scale Data Management — Semester Project
**Student:** Αλμάδι Μικέλα | **AM:** 03400282

The following table declares the use of LLM tools (specifically Claude by Anthropic)
in the context of this project, in accordance with the course submission requirements.

---

## Code 

| Section | Tool | Extent of Use | Completed Without LLM |
|---|---|---|---|
| `explore_crime_schema.py` (diagnostic) | Claude (Anthropic) | Claude wrote data exploration script | Student ran the script on cluster, identified relevant columns (`TIME OCC`, `Premis Desc`, `LAT`, `LON`) for use in Queries 1, 2, 4 |
| `inspect_geojson.py` (diagnostic) | Claude (Anthropic) | Clause wrote inspection script | Student ran the script on cluster, identified relevant fields (`ZCTA20`, `POP20`) for use in Query 3 |
| `query_1.py` (DataFrame no-UDF, DataFrame with UDF, RDD) | Claude (Anthropic) | Clausde created initial guide for the script, and debugged student's script later: debugged the UDF serialization issue and suggested `cache()` for fair comparison | Student filled in blanks in initial skeleton: implemented the `when/otherwise` classification logic, `reduceByKey` aggregation, and validated all three results on cluster |
| `query_2.py` (DataFrame API, SQL API) | Claude (Anthropic) | Claude created initial guide for the script, debugged the date parsing approach and suggested `substring` over `to_date` for non-standard formats | Student wrote script, implemented SQL approach, month name mapping, and validated results on cluster |
| `query_3.py` (DataFrame API, RDD API) | Claude (Anthropic) | Claude suggested the GeoJSON parsing and join logic; Claude wrote initial script guide; debugged the income column formatting issue (silent NULL cast) and suggested `regexp_replace` fix | Student filled blanks in the guide, identified the `$` and `,` parsing bug through log analysis, implemented `isdigit()` validation in RDD, and validated results on cluster | 
| `query_4.py` (DataFrame API, Haversine UDF) | Claude (Anthropic) | Claude identified the coordinate swap bug through log analysis; debugged the police station coordinate column names (`X`/`Y` vs `LAT`/`LON`) and the coordinate swap issue | Student decided to use Havensine distance for calculations; wrote the initial skeleton with cross join and window function logic; fixed the Earth radius typo (6471->6371), and validated results on cluster |
| `convert_to_parquet.py` | Claude (Anthropic) | - | Student wrote initial script for CSV-to-Parquet conversion validated Parquet output on HDFS |
| `query_1_parquet.py` | Claude (Anthropic) | Student wrote the initial skeleton for CSV vs Parquet comparison; Claude ensured logic consistency with original `query_1.py` | Student decided to use parquet file format for comparison; Verified identical results between CSV and Parquet runs and validated speedup on cluster |
| `query_3_joins.py` | Claude (Anthropic) | Claude suggested caching both DataFrames before strategy runs for fair timing | Student wrote the initial skeleton for join strategy comparison; Ran all 5 strategies on cluster and interpreted physical plan output |
| `query_4_joins.py` | Claude (Anthropic) | - | Student wrote the initial skeleton for join strategy comparison ; ran all 5 strategies on cluster and interpreted physical plan output |
| Report | Claude (Anthropic) | Generated LaTeX structure, checked for typos, gathered key takeaways for conclusion | Full text written by student | 
---
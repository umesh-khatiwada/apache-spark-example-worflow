"""
# Module 10: Complex Types (Arrays, Maps, Structs) & Nested JSON Processing

## 1. What It Is
Complex types allow representing non-scalar, hierarchical, and schema-within-schema structures in Spark DataFrames.
Key complex types include:
- **Structs**: Fixed-field records (similar to an object or dictionary).
- **Arrays**: Ordered collections of homogenous elements.
- **Maps**: Key-value pairs with arbitrary dynamic keys.

## 2. Why It Matters
Modern data architectures ingest semi-structured telemetry, clickstream logs, IoT metrics, and API payloads in JSON format.
Mastering functions like `explode()`, `posexplode()`, `from_json()`, and dot-notation traversal allows flattening or nesting data without writing cumbersome iterative Python scripts.

## 3. Concept Diagram
```
  Nested Clickstream Record:
  {
    "event_id": "evt_001",
    "metadata": { "product_id": 42, "search_query": "laptop" }  <-- STRUCT
    "tags": ["promo", "electronic"]                              <-- ARRAY
  }

  Flattening via Dot Notation & Explode:
  - df.select("metadata.product_id")                 ---> scalar column
  - df.select("event_id", explode("tags").alias("tag"))
    Row 1: evt_001 | promo
    Row 2: evt_001 | electronic
```

## 7. Common Mistakes & Gotchas
- **explode() drops rows with empty or null arrays**: Calling `explode()` on a null or empty array silently deletes the entire row. Use `explode_outer()` to preserve the row with a `NULL` value.
- **Schema mismatch in from_json()**: If your JSON string has fields not defined in the `from_json(col, schema)`, Spark silently drops them; if types mismatch, it assigns `NULL`.
- **Calling explode() multiple times in one select**: Calling `df.select(explode("arr1"), explode("arr2"))` throws `AnalysisException: Only one generator allowed per select clause`. Use consecutive `.withColumn()` or `lateral view` instead.

## 8. Official Documentation
- PySpark Complex Types: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
- JSON Functions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#json-functions
"""

import sys
import os

# Project root setup (compatible with both standalone execution and Jupyter notebooks)
def _find_project_root():
    cur = os.path.dirname(os.path.abspath(__file__)) if "__file__" in locals() else os.getcwd()
    while cur != os.path.dirname(cur):
        if os.path.exists(os.path.join(cur, "data", "raw")):
            return cur
        cur = os.path.dirname(cur)
    if os.path.exists("/home/jovyan/work/data/raw"):
        return "/home/jovyan/work"
    return os.path.abspath(os.getcwd())

project_root = _find_project_root()
os.chdir(project_root)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from scripts.spark_session import get_spark_session
from pyspark.sql.functions import (
    col, explode, explode_outer, posexplode, 
    from_json, to_json, schema_of_json, lit,
    collect_list, collect_set, size, array_contains, struct
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def run_module_10():
    print("=================================================================")
    print(" Module 10: Complex Types (Arrays, Maps, Structs) & Nested JSON  ")
    print("=================================================================")

    spark = get_spark_session("Module10-ComplexTypes")

    # 1. Loading Nested JSON Clickstream Data
    print("\n[Step 1] Ingesting Clickstream JSON with Nested Structs...")
    df_clickstream = spark.read.json("data/raw/clickstream.json")
    print("[*] Clickstream Schema (Notice nested 'metadata' struct):")
    df_clickstream.printSchema()

    # Accessing nested fields directly via dot notation
    df_flattened = df_clickstream.select(
        col("event_id"),
        col("event_type"),
        col("customer_id"),
        col("metadata.product_id").alias("meta_product_id"),
        col("metadata.search_query").alias("meta_search_query"),
        col("metadata.referrer").alias("meta_referrer")
    )
    print("[✓] Flattened Struct Fields:")
    df_flattened.filter(col("meta_search_query").isNotNull()).show(5)

    # 2. Working with Arrays: collect_list, collect_set, size, array_contains
    print("\n[Step 2] Building and Querying Arrays (Customer Browsing History)...")
    customer_history = (
        df_clickstream
        .filter(col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            collect_list("event_type").alias("all_events"),
            collect_set("event_type").alias("unique_events"),
            size(collect_list("event_type")).alias("event_count")
        )
    )
    
    # Filter customers who performed a checkout_step
    checkout_users = customer_history.filter(array_contains(col("unique_events"), "checkout_step"))
    print(f"[✓] Customers with 'checkout_step' in event array:")
    checkout_users.show(5, truncate=True)

    # 3. Exploding Arrays: explode vs explode_outer vs posexplode
    print("\n[Step 3] Exploding Arrays: explode vs explode_outer vs posexplode...")
    sample_tags_data = [
        (101, ["promo", "summer", "sale"]),
        (102, ["clearance"]),
        (103, []),      # Empty array
        (104, None)     # Null array
    ]
    df_tags = spark.createDataFrame(sample_tags_data, ["item_id", "tags"])

    print("--- A) explode() drops rows 103 & 104 ---")
    df_tags.select("item_id", explode("tags").alias("tag")).show()

    print("--- B) explode_outer() keeps rows 103 & 104 with NULL ---")
    df_tags.select("item_id", explode_outer("tags").alias("tag")).show()

    print("--- C) posexplode() includes zero-indexed array position ---")
    df_tags.select("item_id", posexplode("tags").alias("pos", "tag")).show()

    # 4. JSON Serialization and Deserialization: from_json and to_json
    print("\n[Step 4] Converting Struct to JSON string (to_json) & Parsing back (from_json)...")
    # Pack columns into a single JSON struct
    packed_df = df_flattened.limit(3).withColumn(
        "json_payload",
        to_json(struct("event_id", "event_type", "meta_referrer"))
    )
    packed_df.select("json_payload").show(truncate=False)

    # Parse back from JSON string with explicit schema
    payload_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("meta_referrer", StringType(), True)
    ])
    
    unpacked_df = packed_df.withColumn("parsed_struct", from_json(col("json_payload"), payload_schema))
    print("[✓] Unpacked struct from JSON payload:")
    unpacked_df.select("parsed_struct.*").show()

    print("\n[*] Module 10 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_10()

"""
## 5. Expected Output
=================================================================
 Module 10: Complex Types (Arrays, Maps, Structs) & Nested JSON  
=================================================================

[Step 1] Ingesting Clickstream JSON with Nested Structs...
[*] Clickstream Schema (Notice nested 'metadata' struct):
root
 |-- customer_id: long (nullable = true)
 |-- device_type: string (nullable = true)
 |-- event_id: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- ip_address: string (nullable = true)
 |-- metadata: struct (nullable = true)
 |    |-- product_id: long (nullable = true)
 |    |-- referrer: string (nullable = true)
 |    |-- search_query: string (nullable = true)
...

[Step 2] Building and Querying Arrays (Customer Browsing History)...
...

[Step 3] Exploding Arrays: explode vs explode_outer vs posexplode...
--- A) explode() drops rows 103 & 104 ---
+-------+---------+
|item_id|      tag|
+-------+---------+
|    101|    promo|
|    101|   summer|
|    101|     sale|
|    102|clearance|
+-------+---------+

--- B) explode_outer() keeps rows 103 & 104 with NULL ---
+-------+---------+
|item_id|      tag|
+-------+---------+
|    101|    promo|
|    101|   summer|
|    101|     sale|
|    102|clearance|
|    103|     NULL|
|    104|     NULL|
+-------+---------+

--- C) posexplode() includes zero-indexed array position ---
+-------+---+---------+
|item_id|pos|      tag|
+-------+---+---------+
|    101|  0|    promo|
|    101|  1|   summer|
|    101|  2|     sale|
|    102|  0|clearance|
+-------+---+---------+

[Step 4] Converting Struct to JSON string (to_json) & Parsing back (from_json)...
...

[*] Module 10 completed successfully.

## 6. Try It Yourself (Exercises)
1. Use `schema_of_json(lit('{"user": "alice", "score": 98.5}'))` to automatically infer a JSON schema string.
2. Group clickstream events by `session_id` and construct a Map column mapping `event_type` -> `count` using `map_from_entries`.
3. Use `transform(col("all_events"), lambda x: upper(x))` to capitalize every element inside an array column without exploding it.
"""

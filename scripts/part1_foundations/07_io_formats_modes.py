"""
# Module 07: I/O Formats, Partitioning, Write Modes & Corrupt Record Handling

## 1. What It Is
Spark supports reading and writing a wide range of file formats (CSV, JSON, Parquet, ORC, Avro, Text).
It provides fine-grained controls over storage layouts (`partitionBy`), write behaviors (`overwrite`, `append`, `ignore`, `errorIfExists`), and resilient parsing modes (`PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`) to isolate corrupt data without crashing production jobs.

## 2. Why It Matters
Row-based formats like CSV and JSON require full-file scans and string parsing on every query.
Columnar formats like Parquet and ORC store columnar statistics (min/max/null count), utilize dictionary encoding and Snappy compression, and allow predicate pushdown and column pruning, often speeding up queries by 10x-100x.

## 3. Concept Diagram
```
  Format Comparison:
  CSV / JSON (Row-based, Text):
    [Row 1: id, name, price]
    [Row 2: id, name, price]  ---> Must read ALL columns to parse any one column!

  Parquet / ORC (Columnar, Binary, Compressed):
    [Column: id   | min: 1, max: 1000]
    [Column: name | dictionary encoded] ---> Can read ONLY 'price', skipping all other columns!
    [Column: price| Snappy compressed]
```

## 7. Common Mistakes & Gotchas
- **Inferring schema on large CSV/JSON in production**: `inferSchema=True` scans the entire dataset *twice* before processing begins. Always pass an explicit `StructType` in production pipelines.
- **Over-partitioning with `partitionBy()`**: Partitioning by high-cardinality columns (e.g. `timestamp` or `customer_id`) creates millions of tiny directories and files, leading to metastore exhaustion and the small-files problem.
- **Using default write mode without understanding overwrite**: In `mode("overwrite")`, Spark deletes the target folder before writing unless dynamic partition overwrite is configured.

## 8. Official Documentation
- Spark SQL Data Sources Guide: https://spark.apache.org/docs/latest/sql-data-sources.html
- Parquet Files Guide: https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
"""

import sys
import os
import shutil

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

def run_module_07():
    print("=================================================================")
    print(" Module 07: I/O Formats, Write Modes & Corrupt Record Handling   ")
    print("=================================================================")

    spark = get_spark_session("Module07-IOFormatsModes")
    target_base = "data/warehouse/module07_io_demo"
    if os.path.exists(target_base):
        shutil.rmtree(target_base)

    # 1. Reading JSON and Writing as Optimized Columnar Parquet
    print("\n[Step 1] Ingesting JSON and writing to Columnar Parquet with partitionBy...")
    df_orders = spark.read.json("data/raw/orders.json")
    
    parquet_target = f"{target_base}/orders_partitioned_parquet"
    (
        df_orders.write
        .mode("overwrite")
        .partitionBy("status")
        .parquet(parquet_target)
    )
    print(f"[✓] Successfully partitioned orders by 'status' into: {parquet_target}")
    
    # Inspect generated partitions
    partitions = [d for d in os.listdir(parquet_target) if os.path.isdir(os.path.join(parquet_target, d))]
    print(f"    - Discovered partition directories: {partitions}")

    # 2. Benchmark Column Pruning on Parquet vs JSON
    print("\n[Step 2] Demonstrating Column Pruning & Partition Pruning...")
    # Spark reads ONLY the 'COMPLETED' partition directory and ONLY the 'total_amount' column
    df_completed = (
        spark.read.parquet(parquet_target)
        .filter(col("status") == "COMPLETED")
        .select("order_id", "total_amount")
    )
    print(f"[✓] Partition pruning query count: {df_completed.count()}")
    print("    - Physical Plan showing PartitionFilters and PushedFilters:")
    df_completed.explain()

    # 3. Demonstrating Write Modes (append, overwrite, ignore, errorIfExists)
    print("\n[Step 3] Demonstrating Write Modes...")
    single_record = df_orders.limit(1)
    
    # Append
    single_record.write.mode("append").parquet(f"{target_base}/orders_append_demo")
    print(f"    - Append pass 1 count: {spark.read.parquet(f'{target_base}/orders_append_demo').count()}")
    single_record.write.mode("append").parquet(f"{target_base}/orders_append_demo")
    print(f"    - Append pass 2 count: {spark.read.parquet(f'{target_base}/orders_append_demo').count()} (doubled as expected)")

    # 4. Corrupt Record Handling (PERMISSIVE with _corrupt_record)
    print("\n[Step 4] Handling Corrupt & Malformed Records Gracefully...")
    corrupt_data_dir = f"{target_base}/corrupt_sample"
    os.makedirs(corrupt_data_dir, exist_ok=True)
    with open(f"{corrupt_data_dir}/data.csv", "w") as f:
        f.write("id,name,amount\n")
        f.write("1,Valid Customer,150.0\n")
        f.write("MALFORMED_ROW_CORRUPT_DATA_WITHOUT_DELIMITERS\n")
        f.write("2,Second Customer,220.5\n")
        f.write("3,Broken Row,NOT_A_NUMBER\n")

    # Define schema including special `_corrupt_record` column
    resilient_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("_corrupt_record", StringType(), True)
    ])

    df_corrupt_handled = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(resilient_schema)
        .csv(corrupt_data_dir)
    )

    print("[✓] Corrupt record handling results:")
    df_corrupt_handled.show(truncate=False)

    print("\n[*] Module 07 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_07()

"""
## 5. Expected Output
=================================================================
 Module 07: I/O Formats, Write Modes & Corrupt Record Handling   
=================================================================

[Step 1] Ingesting JSON and writing to Columnar Parquet with partitionBy...
[✓] Successfully partitioned orders by 'status' into: data/warehouse/module07_io_demo/orders_partitioned_parquet
    - Discovered partition directories: ['status=CANCELLED', 'status=COMPLETED', ...]

[Step 2] Demonstrating Column Pruning & Partition Pruning...
[✓] Partition pruning query count: ...
    - Physical Plan showing PartitionFilters and PushedFilters:
...

[Step 3] Demonstrating Write Modes...
    - Append pass 1 count: 1
    - Append pass 2 count: 2 (doubled as expected)

[Step 4] Handling Corrupt & Malformed Records Gracefully...
[✓] Corrupt record handling results:
+----+---------------+------+----------------------------------------------+
|id  |name           |amount|_corrupt_record                               |
+----+---------------+------+----------------------------------------------+
|1   |Valid Customer |150.0 |NULL                                          |
|NULL|NULL           |NULL  |MALFORMED_ROW_CORRUPT_DATA_WITHOUT_DELIMITERS |
|2   |Second Customer|220.5 |NULL                                          |
|NULL|NULL           |NULL  |3,Broken Row,NOT_A_NUMBER                     |
+----+---------------+------+----------------------------------------------+

[*] Module 07 completed successfully.

## 6. Try It Yourself (Exercises)
1. Re-read the corrupt CSV with `.option("mode", "DROPMALFORMED")`. Verify how many rows survive in the output.
2. Re-read the corrupt CSV with `.option("mode", "FAILFAST")`. Observe the exact JVM exception thrown when the corrupt row is encountered.
3. Compare the disk size of `data/raw/orders.json` vs the unpartitioned Parquet equivalent. How much compression was achieved?
"""

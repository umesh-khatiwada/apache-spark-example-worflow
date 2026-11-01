"""
# Module 33: Open Table Formats (Delta Lake & Apache Iceberg ACID Transactions)

## 1. What It Is
Modern Open Table Formats (Delta Lake and Apache Iceberg) add an ACID transactional metadata layer on top of raw Parquet files stored in cloud object storage or HDFS.
They provide **ACID transactions**, **Time Travel** (querying historical table snapshots), **Atomic MERGE (Upserts)**, **Schema Evolution**, and **Compaction**.

## 2. Why It Matters
Without table formats, writing to raw Parquet files is not transactional: a job failing midway leaves corrupted partial files, concurrent readers see uncommitted dirty data, and updating individual records requires rewriting entire partitions.
Delta Lake and Iceberg enable reliable "Lakehouse" architectures directly on cheap object storage.

## 3. Concept Diagram
```
  Delta Lake Architecture:
  table_dir/
  ├── _delta_log/
  │   ├── 00000000000000000000.json  ──► Commit 0 (Initial append: added part-0.parquet)
  │   ├── 00000000000000000001.json  ──► Commit 1 (MERGE: deleted old file, added new part-1.parquet)
  │   └── 00000000000000000010.checkpoint.parquet ──► Periodic state compaction
  ├── part-00000.parquet
  └── part-00001.parquet
```

## 7. Common Mistakes & Gotchas
- **Forgetting delta-spark jar packages**: In vanilla Spark installations, reading format `"delta"` without the `io.delta:delta-spark_2.12` package throws `ClassNotFoundException: delta`.
- **Modifying Parquet files directly outside Delta**: Deleting or writing files inside the folder without committing via Delta bypasses the `_delta_log`, causing metadata corruption.
- **Neglecting VACUUM**: Delta retains old Parquet files for Time Travel. If you never run `VACUUM`, storage costs will grow continuously.

## 8. Official Documentation
- Delta Lake Documentation: https://docs.delta.io/latest/index.html
- Apache Iceberg Spark Quickstart: https://iceberg.apache.org/spark-quickstart/
"""

import sys
import os
import shutil

# Project root setup (compatible with both standalone execution and Jupyter notebooks)
if "__file__" in locals():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
else:
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../..") if "notebooks" in os.getcwd() else os.getcwd())
    os.chdir(project_root)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
from scripts.spark_session import get_spark_session
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def run_module_33():
    print("=================================================================")
    print(" Module 33: Open Table Formats (Delta Lake ACID & Time Travel)   ")
    print("=================================================================")

    # Initialize SparkSession with Delta Lake extensions
    builder = (
        SparkSession.builder
        .appName("Module33-DeltaLake")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    delta_table_path = "data/warehouse/module33_delta_table"
    if os.path.exists(delta_table_path):
        shutil.rmtree(delta_table_path)

    # 1. Writing Initial Delta Table (Commit Version 0)
    print("\n[Step 1] Ingesting customer sample and saving as Delta Table (Version 0)...")
    df_customers = (
        spark.read.option("header", "true").csv("data/raw/customers.csv")
        .select("customer_id", "first_name", "loyalty_tier")
        .limit(5)
    )

    df_customers.write.format("delta").mode("overwrite").save(delta_table_path)
    print(f"[✓] Created Delta table at: {delta_table_path}")
    print("[*] Table at Version 0:")
    spark.read.format("delta").load(delta_table_path).show()

    # Inspect _delta_log directory
    log_dir = os.path.join(delta_table_path, "_delta_log")
    print(f"[*] Delta Log Commit Files: {os.listdir(log_dir)}")

    # 2. Atomic Update / Append (Commit Version 1)
    print("\n[Step 2] Executing Update & Append (Commit Version 1)...")
    # Simulate an upgrade: Customer 1 upgrades to Platinum, and Customer 999 joins
    updates = [
        ("1", "Danielle (Updated)", "Platinum"),
        ("999", "NewCustomer", "Gold")
    ]
    df_updates = spark.createDataFrame(updates, ["customer_id", "first_name", "loyalty_tier"])

    from delta.tables import DeltaTable
    delta_tbl = DeltaTable.forPath(spark, delta_table_path)

    # Atomic MERGE (Upsert)
    (
        delta_tbl.alias("target")
        .merge(df_updates.alias("source"), "target.customer_id = source.customer_id")
        .whenMatchedUpdate(set={"first_name": col("source.first_name"), "loyalty_tier": col("source.loyalty_tier")})
        .whenNotMatchedInsert(values={"customer_id": col("source.customer_id"), "first_name": col("source.first_name"), "loyalty_tier": col("source.loyalty_tier")})
        .execute()
    )

    print("[✓] MERGE Upsert executed atomically.")
    print("[*] Current Table State (Version 1):")
    spark.read.format("delta").load(delta_table_path).show()

    # 3. Time Travel: Querying Version 0 vs Version 1
    print("\n[Step 3] Time Travel: Querying Historical Table Snapshots...")
    v0_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
    print("[✓] Time Travel to Version 0 (Customer 1 still has original tier):")
    v0_df.filter("customer_id = '1'").show()

    v1_df = spark.read.format("delta").option("versionAsOf", 1).load(delta_table_path)
    print("[✓] Time Travel to Version 1 (Customer 1 updated to Platinum):")
    v1_df.filter("customer_id = '1'").show()

    # 4. Inspecting Transaction History
    print("\n[Step 4] Querying Transaction History Metadata:")
    delta_tbl.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

    print("\n[*] Module 33 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_33()

"""
## 5. Expected Output
=================================================================
 Module 33: Open Table Formats (Delta Lake ACID & Time Travel)   
=================================================================

[Step 1] Ingesting customer sample and saving as Delta Table (Version 0)...
[✓] Created Delta table at: data/warehouse/module33_delta_table
[*] Table at Version 0:
+-----------+----------+------------+
|customer_id|first_name|loyalty_tier|
+-----------+----------+------------+
|...        |...       |...         |
+-----------+----------+------------+

[Step 2] Executing Update & Append (Commit Version 1)...
[✓] MERGE Upsert executed atomically.
[*] Current Table State (Version 1):
...

[Step 3] Time Travel: Querying Historical Table Snapshots...
[✓] Time Travel to Version 0 (Customer 1 still has original tier):
...
[✓] Time Travel to Version 1 (Customer 1 updated to Platinum):
...

[*] Module 33 completed successfully.

## 6. Try It Yourself (Exercises)
1. Add a new column `credit_score` with `.option("mergeSchema", "true")`. Observe how Delta Lake evolves the schema without rewriting existing data.
2. Run `delta_tbl.vacuum(retentionHours=168)`. What happens to Parquet files associated with historical versions outside the retention window?
3. Contrast Delta Lake's `_delta_log` JSON model vs Apache Iceberg's metadata tree (Snapshot -> Manifest List -> Manifest File -> Data Files).
"""

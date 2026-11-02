"""
# Module 17: Partitioning, Coalesce, Bucketing & Small-Files Compaction

## 1. What It Is
Partitioning controls how data is physically split across cluster executor nodes and storage directories.
Spark offers several partitioning mechanisms:
- **repartition(N)**: Full shuffle that evenly balances data across N partitions or hashes by specific columns.
- **coalesce(N)**: Narrow transformation that combines existing partitions without a shuffle (downwards only).
- **Bucketing**: Pre-shuffles and pre-sorts data into a fixed number of hash buckets on write to eliminate shuffles during future joins and aggregations.

## 2. Why It Matters
Data skew, shuffle spills, and the "small-files problem" (millions of kilobyte-sized files overwhelming HDFS / S3 / Blob metadata)
can all be resolved by mastering partitioning. Using `coalesce()` before writing saves storage cost, while bucketing eliminates shuffles on repetitive daily joins.

## 3. Concept Diagram
```
  repartition(2 -> 4): Full Network Shuffle (Increases Partitions)
    [P0] ───\ /───► [P0]
             X
    [P1] ───/ \───► [P1], [P2], [P3]

  coalesce(4 -> 2): Narrow Combine (Zero Shuffle, Decreases Partitions)
    [P0] + [P1] ──► [New P0]  (Local task merge)
    [P2] + [P3] ──► [New P1]
```

## 7. Common Mistakes & Gotchas
- **Using coalesce() to increase partitions**: Calling `df.coalesce(10)` on a DataFrame with 2 partitions does nothing; it silently remains 2 partitions. You MUST use `repartition()` to increase partition count.
- **Calling repartition() immediately followed by coalesce()**: This triggers an expensive full cluster shuffle only to collapse it down immediately.
- **Bucketing without saving as a managed/external table**: Bucketing metadata is only preserved when written via `.saveAsTable()`. Writing to raw parquet files with `.parquet()` ignores bucket metadata on subsequent reads.

## 8. Official Documentation
- DataFrame Partitioning: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.repartition.html
- Bucketing Guide: https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html#bucketing-sorting-and-partitioning
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
from pyspark.sql.functions import col

def run_module_17():
    print("=================================================================")
    print(" Module 17: Partitioning, Coalesce, Bucketing & Compaction       ")
    print("=================================================================")

    spark = get_spark_session("Module17-PartitioningMastery")
    target_dir = "data/warehouse/module17_partitions"
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)

    df_orders = spark.read.json("data/raw/orders.json")

    # 1. repartition() vs coalesce()
    print("\n[Step 1] Comparing repartition() vs coalesce()...")
    initial_parts = df_orders.rdd.getNumPartitions()
    print(f"[*] Initial Orders Partitions: {initial_parts}")

    # Increase partitions: Must use repartition (triggers shuffle)
    df_repart = df_orders.repartition(8)
    print(f"[✓] After repartition(8): {df_repart.rdd.getNumPartitions()} partitions (Full Shuffle)")

    # Decrease partitions: Use coalesce (no shuffle)
    df_coalesced = df_repart.coalesce(2)
    print(f"[✓] After coalesce(2):   {df_coalesced.rdd.getNumPartitions()} partitions (Narrow merge, Zero Shuffle)")

    # 2. Partitioning by Column (Hash Partitioning)
    print("\n[Step 2] Repartitioning by Key ('payment_method')...")
    df_by_payment = df_orders.repartition(4, "payment_method")
    print(f"[✓] Repartitioned by payment_method into {df_by_payment.rdd.getNumPartitions()} partitions.")

    # 3. Solving the Small-Files Problem (Compaction Demo)
    print("\n[Step 3] Solving the Small-Files Problem with Compaction...")
    small_files_path = f"{target_dir}/uncompacted_small_files"
    compacted_path = f"{target_dir}/compacted_files"

    # Simulate generating many small files
    df_orders.repartition(16).write.mode("overwrite").parquet(small_files_path)
    small_file_count = len([f for f in os.listdir(small_files_path) if f.endswith(".parquet")])
    print(f"    - Generated {small_file_count} small parquet files in '{small_files_path}'")

    # Compact files: Read back and coalesce to 1 or 2 files
    spark.read.parquet(small_files_path).coalesce(2).write.mode("overwrite").parquet(compacted_path)
    compacted_count = len([f for f in os.listdir(compacted_path) if f.endswith(".parquet")])
    print(f"[✓] Compacted {small_file_count} files into {compacted_count} optimal parquet files in '{compacted_path}'")

    # 4. Bucketing Demo: Pre-sorting and Hash Partitioning on write
    print("\n[Step 4] Bucketing: Pre-sorting and saving as bucketed table...")
    spark.sql("DROP TABLE IF EXISTS bucketed_orders")
    
    (
        df_orders.write
        .mode("overwrite")
        .bucketBy(4, "customer_id")
        .sortBy("total_amount")
        .saveAsTable("bucketed_orders")
    )
    print("[✓] Saved bucketed table 'bucketed_orders' (4 buckets sorted by total_amount).")
    
    # Query bucketed table to observe plan
    df_bucketed_read = spark.table("bucketed_orders").filter(col("customer_id") == 42)
    print("    - Query execution plan on bucketed table:")
    df_bucketed_read.explain()

    print("\n[*] Module 17 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_17()

"""
## 5. Expected Output
=================================================================
 Module 17: Partitioning, Coalesce, Bucketing & Compaction       
=================================================================

[Step 1] Comparing repartition() vs coalesce()...
[*] Initial Orders Partitions: ...
[✓] After repartition(8): 8 partitions (Full Shuffle)
[✓] After coalesce(2):   2 partitions (Narrow merge, Zero Shuffle)

[Step 2] Repartitioning by Key ('payment_method')...
[✓] Repartitioned by payment_method into 4 partitions.

[Step 3] Solving the Small-Files Problem with Compaction...
    - Generated 16 small parquet files in ...
[✓] Compacted 16 files into 2 optimal parquet files in ...

[Step 4] Bucketing: Pre-sorting and saving as bucketed table...
[✓] Saved bucketed table 'bucketed_orders' (4 buckets sorted by total_amount).
    - Query execution plan on bucketed table:
...

[*] Module 17 completed successfully.

## 6. Try It Yourself (Exercises)
1. Write a Python script to check file size distribution across partitions using `spark_partition_id()`.
2. Compare the physical plan of joining two bucketed tables (both bucketed on `customer_id` into 4 buckets) vs unbucketed tables. Did Spark eliminate the `SortMergeJoin` Exchange step?
3. What is the difference between `repartition(10)` vs `repartition(10, "customer_id")`?
"""

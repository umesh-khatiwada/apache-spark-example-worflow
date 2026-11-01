"""
# Module 19: Spark Performance Tuning & Handling Data Skew with Salting

## 1. What It Is
**Data Skew** occurs when data is unevenly distributed across cluster partitions, causing one or two tasks to process millions of records while the remaining tasks sit idle.
**Salting** is an optimization technique where a random integer prefix ("salt") is added to the skewed join key on the large table, while replicating keys on the smaller lookup table, evenly redistributing the skewed keys across all worker nodes.

## 2. Why It Matters
Data skew is the #1 cause of jobs getting stuck at 99% (the "straggler task" problem) or failing with `FetchFailedException` / Executor OOM.
Proper tuning of executor cores (3-5 cores per executor), overhead memory (`spark.executor.memoryOverhead`), and salting turns a failing 2-hour job into a 3-minute success.

## 3. Concept Diagram
```
  Un-salted Skewed Join (Single Straggler Executor):
    Partition 0 (Key: 'Electronics'): 95% of all rows! ──► Executor 1 [Spills to disk, OOM crash!]
    Partition 1 (Key: 'Books'):        3% of rows       ──► Executor 2 [Finishes in 1 sec, idle]
    Partition 2 (Key: 'Beauty'):       2% of rows       ──► Executor 3 [Finishes in 1 sec, idle]

  Salted Join (Key + Random Salt 0..2):
    Partition 0 ('Electronics_0'): 32% ──► Executor 1 [Evenly distributed]
    Partition 1 ('Electronics_1'): 32% ──► Executor 2 [Evenly distributed]
    Partition 2 ('Electronics_2'): 31% ──► Executor 3 [Evenly distributed]
```

## 7. Common Mistakes & Gotchas
- **Setting executor cores too high (> 5 cores)**: Allocating 16 or 32 cores to a single executor causes massive JVM garbage collection pauses. The industry best practice is **4 to 5 cores per executor**.
- **Forgetting memoryOverhead**: Heavy PyArrow operations, off-heap buffers, and shuffle connections require off-heap memory. Always ensure `spark.executor.memoryOverhead` is at least 10% of executor memory (or >= 384MB).
- **Salting without replicating the lookup table**: If you append a random salt `0..2` to table A, you MUST explode/replicate keys in table B across all possible salts `0..2`, otherwise rows will not join!

## 8. Official Documentation
- Tuning Spark Guide: https://spark.apache.org/docs/latest/tuning.html
- Memory Management Overview: https://spark.apache.org/docs/latest/tuning.html#memory-management-overview
"""

import sys
import os
import time

# Project root setup (compatible with both standalone execution and Jupyter notebooks)
if "__file__" in locals():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
else:
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../..") if "notebooks" in os.getcwd() else os.getcwd())
    os.chdir(project_root)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
from scripts.spark_session import get_spark_session
from pyspark.sql.functions import col, lit, concat_ws, floor, rand, explode, array

def run_module_19():
    print("=================================================================")
    print(" Module 19: Performance Tuning Guide & Skew Mitigation (Salting) ")
    print("=================================================================")

    spark = get_spark_session("Module19-TuningSalting", extra_configs={
        "spark.sql.adaptive.enabled": "false", # Temporarily disable AQE to demonstrate manual salting pattern
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.shuffle.partitions": "4"
    })

    # 1. Synthesizing a Severely Skewed Dataset
    print("\n[Step 1] Synthesizing heavily skewed dataset (90% of rows have key 'SKEWED_KEY')...")
    num_rows = 150000
    skewed_records = []
    
    # Generate DataFrame where key "HOT_KEY" accounts for 85% of records
    df_large = (
        spark.range(0, num_rows, 1, numPartitions=4)
        .withColumn(
            "join_key",
            when(col("id") < num_rows * 0.85, lit("HOT_KEY")).otherwise(concat_ws("_", lit("NORMAL"), col("id") % 100))
        )
        .withColumn("value", col("id") * 1.5)
    )

    df_lookup = (
        spark.createDataFrame([("HOT_KEY", "High Volume Promo Item")] + [(f"NORMAL_{i}", f"Standard Item {i}") for i in range(100)], ["join_key", "item_desc"])
        .repartition(4, "join_key")
    )

    # 2. Benchmark Un-salted Skewed Join (Demonstrates partition imbalance)
    print("\n[Step 2] Executing Un-salted Skewed Join (All HOT_KEY records forced into 1 partition)...")
    t0 = time.time()
    unsalted_res = df_large.join(df_lookup, on="join_key").count()
    time_unsalted = time.time() - t0
    print(f"[✓] Un-salted join completed {unsalted_res} rows in {time_unsalted:.4f}s")

    # 3. Applying Salting Technique
    print("\n[Step 3] Applying Salting Technique (Salt factor = 4)...")
    SALT_FACTOR = 4

    # A) Salt the large table by appending a random integer 0..SALT_FACTOR-1
    df_large_salted = df_large.withColumn("salt", floor(rand() * SALT_FACTOR)).withColumn(
        "salted_key",
        concat_ws("_", col("join_key"), col("salt"))
    )

    # B) Replicate / Explode the lookup table across all possible salt values [0, 1, 2, 3]
    salt_array = array([lit(i) for i in range(SALT_FACTOR)])
    df_lookup_replicated = (
        df_lookup
        .withColumn("salt", explode(salt_array))
        .withColumn("salted_key", concat_ws("_", col("join_key"), col("salt")))
    )

    # C) Execute Salted Join
    t0 = time.time()
    salted_res = df_large_salted.join(df_lookup_replicated, on="salted_key").count()
    time_salted = time.time() - t0
    print(f"[✓] Salted join completed {salted_res} rows in {time_salted:.4f}s")
    print(f"    - Notice: Both methods produce identical counts ({unsalted_res} == {salted_res})")
    print(f"    - On large clusters with multi-GB skew, salting prevents Executor OOM and stage stalls!")

    # 4. Executor Sizing Rules Reference
    print("\n[Step 4] Production Sizing Rules Summary:")
    print("    1. Cores per Executor: 4 to 5 (avoid 1 core = poor parallelism; avoid >5 = GC bottleneck)")
    print("    2. Memory per Executor: 16G to 32G per executor (sweet spot for GC pause latency)")
    print("    3. Memory Overhead: max(384MB, 0.10 * executorMemory)")
    print("    4. Shuffle Partitions: Aim for 100MB to 200MB of uncompressed data per partition")

    print("\n[*] Module 19 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    from pyspark.sql.functions import when # ensure imported
    run_module_19()

"""
## 5. Expected Output
=================================================================
 Module 19: Performance Tuning Guide & Skew Mitigation (Salting) 
=================================================================

[Step 1] Synthesizing heavily skewed dataset (90% of rows have key 'SKEWED_KEY')...

[Step 2] Executing Un-salted Skewed Join (All HOT_KEY records forced into 1 partition)...
[✓] Un-salted join completed 150000 rows in ...s

[Step 3] Applying Salting Technique (Salt factor = 4)...
[✓] Salted join completed 150000 rows in ...s
    - Notice: Both methods produce identical counts (150000 == 150000)
    - On large clusters with multi-GB skew, salting prevents Executor OOM and stage stalls!

[Step 4] Production Sizing Rules Summary:
    1. Cores per Executor: 4 to 5 (avoid 1 core = poor parallelism; avoid >5 = GC bottleneck)
    2. Memory per Executor: 16G to 32G per executor (sweet spot for GC pause latency)
    3. Memory Overhead: max(384MB, 0.10 * executorMemory)
    4. Shuffle Partitions: Aim for 100MB to 200MB of uncompressed data per partition

[*] Module 19 completed successfully.

## 6. Try It Yourself (Exercises)
1. Write a script to inspect partition sizes using `df.groupBy(spark_partition_id()).count().show()`. How unbalanced was the un-salted DataFrame?
2. Test salting with a larger scale dataset (`--scale 10`). Measure how much faster salting is when memory pressure causes disk spills.
3. How does AQE Skew Join (`spark.sql.adaptive.skewJoin.enabled=true`) automate salting under the hood?
"""

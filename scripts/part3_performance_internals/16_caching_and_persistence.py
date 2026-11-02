"""
# Module 16: Caching and Persistence Strategies (cache vs persist)

## 1. What It Is
Caching stores intermediate computed DataFrame or RDD partitions in executor memory or disk.
Calling `.cache()` is shorthand for `.persist(StorageLevel.MEMORY_AND_DISK)`.
The `.persist()` API allows selecting explicit storage tiers: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`, and `DISK_ONLY`.

## 2. Why It Matters
When a DataFrame is queried multiple times in an iterative algorithm or DAG branch, caching saves Spark from recomputing the entire upstream lineage.
However, **unnecessary caching is an anti-pattern**: it consumes precious execution memory, triggers Java Garbage Collection pauses, and can actually slow down pipelines if data is only scanned once.

## 3. Concept Diagram
```
  Without Cache (Recomputed on every Action):
    Read Parquet ──► Heavy Filter ──► Join ──► Action 1 (count)    [Computes Lineage]
    Read Parquet ──► Heavy Filter ──► Join ──► Action 2 (summary)  [Re-reads and recomputes all!]

  With Cache:
    Read Parquet ──► Heavy Filter ──► Join ──► [ CACHE in RAM ]
                                                     │
                                      ┌──────────────┴──────────────┐
                                      ▼                             ▼
                              Action 1 (count)              Action 2 (summary)
                              (Instant from RAM)            (Instant from RAM)
```

## 7. Common Mistakes & Gotchas
- **Calling `.cache()` without an action**: Caching is lazy. Calling `.cache()` does not load data into memory until an action (e.g. `.count()`) touches the DataFrame.
- **Forgetting to unpersist()**: Leaving unused DataFrames in cache exhausts executor memory, causing subsequent shuffle stages to spill to disk. Always call `df.unpersist()` when finished.
- **Caching single-use DataFrames**: If a DataFrame is only written once to disk, calling `.cache()` adds pure serialization overhead without any benefit.

## 8. Official Documentation
- RDD Persistence: https://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence
- Storage Levels: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.persist.html
"""

import sys
import os
import time

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
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col

def run_module_16():
    print("=================================================================")
    print(" Module 16: Caching & Persistence (Storage Levels & Benchmarks)  ")
    print("=================================================================")

    spark = get_spark_session("Module16-CachingPersistence")

    # Ingest orders and simulate an expensive transformation pipeline
    df_raw = spark.read.json("data/raw/orders.json")
    df_expensive = (
        df_raw
        .filter(col("status") == "COMPLETED")
        .filter(col("total_amount") > 50.0)
        .groupBy("customer_id", "payment_method")
        .agg({"total_amount": "sum", "order_id": "count"})
    )

    # 1. Benchmark Without Caching (Lineage recomputed on every action)
    print("\n[Step 1] Running 2 consecutive actions WITHOUT caching...")
    t0 = time.time()
    count_1 = df_expensive.count()
    time_action1 = time.time() - t0

    t0 = time.time()
    count_2 = df_expensive.agg({"sum(total_amount)": "max"}).collect()
    time_action2_uncached = time.time() - t0

    print(f"    - Action 1 (Uncached): {time_action1:.4f}s")
    print(f"    - Action 2 (Uncached Recomputation): {time_action2_uncached:.4f}s")

    # 2. Benchmark With Caching
    print("\n[Step 2] Applying .persist(StorageLevel.MEMORY_AND_DISK)...")
    df_expensive.persist(StorageLevel.MEMORY_AND_DISK)
    
    # First action materializes the cache
    t0 = time.time()
    df_expensive.count()
    time_cache_warmup = time.time() - t0

    # Second action reads directly from RAM
    t0 = time.time()
    df_expensive.agg({"sum(total_amount)": "max"}).collect()
    time_action2_cached = time.time() - t0

    print(f"    - Warmup + Action 1: {time_cache_warmup:.4f}s (Persisted to Memory/Disk)")
    print(f"    - Action 2 (FROM CACHE): {time_action2_cached:.4f}s")
    speedup = time_action2_uncached / max(time_action2_cached, 0.0001)
    print(f"[✓] Cached Action Speedup: {speedup:.1f}x Faster!")

    # 3. Inspecting Storage Level Metadata
    print("\n[Step 3] Inspecting Cache Metadata:")
    print(f"    - Is Cached:       {df_expensive.is_cached}")
    print(f"    - Storage Level:   {df_expensive.storageLevel}")
    print(f"    - Uses Memory:     {df_expensive.storageLevel.useMemory}")
    print(f"    - Uses Disk:       {df_expensive.storageLevel.useDisk}")
    print(f"    - Deserialized:    {df_expensive.storageLevel.deserialized}")

    # 4. Cleaning Up Cache
    print("\n[Step 4] Evicting Cache with unpersist()...")
    df_expensive.unpersist()
    print(f"    - Is Cached after unpersist(): {df_expensive.is_cached}")

    print("\n[*] Module 16 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_16()

"""
## 5. Expected Output
=================================================================
 Module 16: Caching & Persistence (Storage Levels & Benchmarks)  
=================================================================

[Step 1] Running 2 consecutive actions WITHOUT caching...
    - Action 1 (Uncached): ...s
    - Action 2 (Uncached Recomputation): ...s

[Step 2] Applying .persist(StorageLevel.MEMORY_AND_DISK)...
    - Warmup + Action 1: ...s (Persisted to Memory/Disk)
    - Action 2 (FROM CACHE): ...s
[✓] Cached Action Speedup: ...x Faster!

[Step 3] Inspecting Cache Metadata:
    - Is Cached:       True
    - Storage Level:   Disk Memory Deserialized 1x Replicated
    - Uses Memory:     True
    - Uses Disk:       True
    - Deserialized:    True

[Step 4] Evicting Cache with unpersist()...
    - Is Cached after unpersist(): False

[*] Module 16 completed successfully.

## 6. Try It Yourself (Exercises)
1. Contrast `StorageLevel.MEMORY_ONLY` vs `StorageLevel.MEMORY_ONLY_SER`. Which one saves more RAM? Which one has higher CPU unpickling overhead?
2. Open the Spark UI at `http://localhost:4040/storage/` while a DataFrame is cached. How much memory and disk space does the table consume?
3. What is the difference between `df.unpersist(blocking=True)` and `df.unpersist(blocking=False)`?
"""

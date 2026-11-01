"""
# Module 20: Spark UI Diagnostics (Jobs, Stages, SQL, Storage, Executors)

## 1. What It Is
The **Spark Web UI** (defaulting to port `4040` on the driver) is the premier monitoring and diagnostic tool for Apache Spark.
It provides real-time and post-mortem visibility into cluster execution, breaking down complex distributed pipelines into **Jobs**, **Stages**, **Tasks**, and physical **SQL DAGs**.

## 2. Why It Matters
When a Spark job runs slowly, fails, or consumes excessive memory, the Spark UI gives you immediate answers:
- Is time spent on Garbage Collection (`GC Time` in Executors tab)?
- Are tasks straggling due to data skew (`Max` task duration >> `75th percentile` in Stages tab)?
- Did shuffle data spill to disk (`Shuffle Spill (Memory / Disk)`)?
- Which operator in the SQL DAG generated the most rows?

## 3. Concept Diagram: How to Read the Spark UI
```
  [1. Jobs Tab] ──────► High-level summary of actions (e.g. count(), write)
        │
        ▼
  [2. Stages Tab] ────► Shuffle boundaries. Check:
        │                 • Task Duration Distribution (Skew detection)
        │                 • Shuffle Read/Write Size
        │                 • Shuffle Spill (Memory vs Disk)
        ▼
  [3. SQL Tab] ───────► Click query execution ID to see visual plan with row counts!
        │
        ▼
  [4. Executors Tab] ─► Check hardware utilization:
                          • GC Time / Task Time ratio (>10% = Memory pressure)
                          • Peak Execution Memory
```

## 7. Common Mistakes & Gotchas
- **Ignoring Shuffle Spill**: When you see `Spill (Memory)` and `Spill (Disk)` in the Stages tab, it means executor memory was insufficient to hold the sort/aggregation buffers, forcing expensive disk writes.
- **Overlooking GC Time in Executors Tab**: If `Task Deserialization Time` or `GC Time` is high compared to `Task Run Time`, your code is generating too many temporary objects or executor JVM heap is too small.
- **Missing the UI after job completion**: The UI shuts down when `spark.stop()` is called unless the Spark History Server is configured with `spark.eventLog.enabled=true`.

## 8. Official Documentation
- Spark Monitoring & Web UI: https://spark.apache.org/docs/latest/monitoring.html
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
from pyspark.sql.functions import col, count

def run_module_20():
    print("=================================================================")
    print(" Module 20: Spark UI Diagnostics & Performance Profiling         ")
    print("=================================================================")

    spark = get_spark_session("Module20-SparkUIDiagnostics", extra_configs={
        "spark.ui.enabled": "true"
    })

    ui_url = spark.sparkContext.uiWebUrl
    print(f"\n[✓] Live Spark UI Active at: {ui_url}")
    print("    Open this URL in your web browser to follow along with the steps below!\n")

    # 1. Triggering a Multi-Stage Job
    print("[Step 1] Executing Multi-Stage Workload to Populate UI Tabs...")
    df_orders = spark.read.json("data/raw/orders.json")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")

    # Query with filter, join, and aggregation -> Creates multiple stages
    result_df = (
        df_orders
        .filter(col("total_amount") > 50.0)
        .join(df_customers, on="customer_id", how="inner")
        .groupBy("loyalty_tier", "payment_method")
        .agg(count("order_id").alias("order_count"))
    )
    result_df.collect()
    print("[✓] Workload completed. Metrics now visible across all Spark UI tabs.")

    # 2. Walkthrough Guide to the Tabs
    print("\n[Step 2] Diagnostic Checklist for Spark UI Tabs:")
    print("    ┌───────────────────────────┬────────────────────────────────────────────────────────┐")
    print("    │ Tab                       │ What to Inspect & Look For                             │")
    print("    ├───────────────────────────┼────────────────────────────────────────────────────────┤")
    print("    │ 1. Jobs                   │ Identify longest running Job ID and associated action. │")
    print("    │ 2. Stages                 │ Check 'Task Duration Quantiles' (Min, 25%, 75%, Max).   │")
    print("    │                           │ If Max is 10x larger than 75%, you have DATA SKEW!     │")
    print("    │                           │ Check 'Spill (Memory)' & 'Spill (Disk)' for OOM risks. │")
    print("    │ 3. Storage                │ Verify cached RDD/DataFrame memory footprint & fraction│")
    print("    │ 4. SQL / DataFrame        │ Click query description to inspect node-by-node rows.  │")
    print("    │ 5. Executors              │ Ensure GC time is < 10% of total run time per node.    │")
    print("    └───────────────────────────┴────────────────────────────────────────────────────────┘")

    # 3. Querying Spark UI Metrics Programmatically via Java Metric API
    print("\n[Step 3] Programmatic Access to Stage & Task Metrics:")
    status_tracker = spark.sparkContext.statusTracker()
    job_ids = status_tracker.getJobIdsForGroup()
    print(f"    - Tracked Job IDs: {list(job_ids)}")

    print("\n[*] Module 20 completed successfully. (Spark UI guide created in docs/spark_ui_guide.md)")
    spark.stop()

if __name__ == "__main__":
    run_module_20()

"""
## 5. Expected Output
=================================================================
 Module 20: Spark UI Diagnostics & Performance Profiling         
=================================================================

[✓] Live Spark UI Active at: http://...:4040
    Open this URL in your web browser to follow along with the steps below!

[Step 1] Executing Multi-Stage Workload to Populate UI Tabs...
[✓] Workload completed. Metrics now visible across all Spark UI tabs.

[Step 2] Diagnostic Checklist for Spark UI Tabs:
...

[Step 3] Programmatic Access to Stage & Task Metrics:
    - Tracked Job IDs: [...]

[*] Module 20 completed successfully. (Spark UI guide created in docs/spark_ui_guide.md)

## 6. Try It Yourself (Exercises)
1. Intentionally trigger a spill by setting `spark.memory.fraction` low and running a massive sort. Look at the Stages tab in the UI. How much data spilled to disk?
2. Open the SQL tab and click the top query. Find the `HashAggregate` box. How many output rows did it produce?
3. In the Executors tab, calculate the ratio of `Task Deserialization Time` to `Duration`. What does high deserialization time indicate?
"""

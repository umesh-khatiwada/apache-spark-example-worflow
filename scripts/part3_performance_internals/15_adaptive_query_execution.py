"""
# Module 15: Adaptive Query Execution (AQE) Dynamic Runtime Optimization

## 1. What It Is
**Adaptive Query Execution (AQE)** is a query re-optimization framework introduced in Spark 3.0 that adapts query execution plans at runtime based on actual stage runtime statistics.
Instead of relying strictly on static compile-time estimates, AQE pauses at shuffle boundaries to inspect real partition sizes and dynamically modifies the physical plan.

## 2. Why It Matters
In traditional query planning, estimating intermediate shuffle sizes accurately is difficult due to filters, user-defined transformations, and data skew.
AQE solves three of Spark's biggest historical pain points:
1. **Dynamic Partition Coalescing**: Automatically merges hundreds of tiny shuffle partitions into optimal partition sizes.
2. **Dynamic Join Strategy Switching**: Flips a Sort-Merge Join into a fast Broadcast Hash Join if filtering reduced one table below the broadcast threshold.
3. **Dynamic Skew Join Optimization**: Splits massive skewed partitions into smaller sub-partitions to eliminate stragglers.

## 3. Concept Diagram
```
  Stage 1 (Map Phase) ──► Computes Runtime Partition Statistics
                                      │
                                      ▼
                        AQE Re-Optimization Engine
        ┌─────────────────────────────┼─────────────────────────────┐
        ▼                             ▼                             ▼
  [ Coalesce Partitions ]     [ Convert to Broadcast ]      [ Split Skewed Partitions ]
  Combines 200 tiny parts     Sort-Merge -> Broadcast       Divides skewed key across
  into 2 target partitions    Hash Join on the fly          multiple worker tasks
```

## 7. Common Mistakes & Gotchas
- **Expecting AQE to optimize before shuffle boundaries**: AQE can only inspect statistics and re-optimize at shuffle Exchange stages. Queries with no shuffles (pure map-side pipelines) cannot trigger AQE.
- **Disabling AQE accidentally**: When troubleshooting custom partition sizes or static join hints, developers sometimes disable AQE (`spark.sql.adaptive.enabled = false`), which degrades cluster-wide resource efficiency.
- **Skew threshold tuning**: If the skew multiplier (`spark.sql.adaptive.skewJoin.skewedPartitionFactor`) is set too high, AQE will not recognize moderate skew that still causes task stragglers.

## 8. Official Documentation
- Adaptive Query Execution Guide: https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution
"""

import sys
import os

# Project root setup (compatible with both standalone execution and Jupyter notebooks)
if "__file__" in locals():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
else:
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../..") if "notebooks" in os.getcwd() else os.getcwd())
    os.chdir(project_root)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
from scripts.spark_session import get_spark_session
from pyspark.sql.functions import col

def run_module_15():
    print("=================================================================")
    print(" Module 15: Adaptive Query Execution (AQE) Dynamic Optimization ")
    print("=================================================================")

    # Initialize SparkSession with AQE explicitly enabled
    spark = get_spark_session("Module15-AQE-DeepDive", extra_configs={
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.shuffle.partitions": "50" # Intentionally set higher to observe coalescing
    })

    df_orders = spark.read.json("data/raw/orders.json")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")

    # 1. Dynamic Partition Coalescing Demonstration
    print("\n[Step 1] Observing Dynamic Partition Coalescing...")
    print(f"[*] Configured static 'spark.sql.shuffle.partitions': {spark.conf.get('spark.sql.shuffle.partitions')}")

    # Aggregation triggering shuffle
    agg_df = df_orders.groupBy("payment_method").count()
    
    # Materialize action
    agg_df.collect()
    
    # Inspect final executed plan
    final_plan = agg_df._jdf.queryExecution().executedPlan().toString()
    print("[✓] Executed Plan with AQE:")
    for line in final_plan.splitlines():
        if "AQEShuffleRead" in line or "coalesced" in line or "AdaptiveSparkPlan" in line:
            print(f"    | {line}")

    # 2. Dynamic Join Switching: Converting Sort-Merge Join to Broadcast Hash Join
    print("\n[Step 2] Dynamic Join Strategy Switching (SMJ -> BHJ on Filtered Data)...")
    # Join with a heavily filtered table: initially optimizer does not know filtered size,
    # but AQE sees at runtime that filtered customers is tiny (<10MB) and converts to Broadcast!
    filtered_customers = df_customers.filter(col("loyalty_tier") == "Platinum") # Very few rows
    joined_df = df_orders.join(filtered_customers, on="customer_id", how="inner")
    
    joined_df.collect() # Trigger execution
    
    join_plan_str = joined_df._jdf.queryExecution().executedPlan().toString()
    if "BroadcastHashJoin" in join_plan_str or "AQEShuffleRead" in join_plan_str:
        print("[✓] AQE dynamically optimized join strategy based on runtime statistics!")
    print(f"    - Final Join Plan summary:\n")
    joined_df.explain()

    print("\n[*] Module 15 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_15()

"""
## 5. Expected Output
=================================================================
 Module 15: Adaptive Query Execution (AQE) Dynamic Optimization 
=================================================================

[Step 1] Observing Dynamic Partition Coalescing...
[*] Configured static 'spark.sql.shuffle.partitions': 50
[✓] Executed Plan with AQE:
    | AdaptiveSparkPlan isFinalPlan=true
    | +- == Final Plan ==
    |    +- AQEShuffleRead coalesced

[Step 2] Dynamic Join Strategy Switching (SMJ -> BHJ on Filtered Data)...
[✓] AQE dynamically optimized join strategy based on runtime statistics!
    - Final Join Plan summary:
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   ...

[*] Module 15 completed successfully.

## 6. Try It Yourself (Exercises)
1. Turn off AQE (`spark.conf.set("spark.sql.adaptive.enabled", "false")`) and re-run Step 1. How many partitions are produced?
2. Adjust `spark.sql.adaptive.advisoryPartitionSizeInBytes` to 1MB and observe how AQE calculates target partition counts.
3. Research `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`. How does Spark decide if a partition is "skewed"?
"""

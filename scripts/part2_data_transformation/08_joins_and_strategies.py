"""
# Module 08: Join Types, Join Strategies (Broadcast, Sort-Merge) & Skew Handling

## 1. What It Is
Joins combine two distributed datasets based on matching key expressions.
Spark implements multiple physical join strategies: **Broadcast Hash Join (BHJ)** (which avoids shuffling by sending small tables to all executors),
**Sort Merge Join (SMJ)** (the robust default for large-to-large distributed datasets requiring hashing and sorting), and **Shuffle Hash Join**.

## 2. Why It Matters
Joins are the single most expensive operation in distributed data processing.
Choosing the wrong join strategy or neglecting data skew causes massive network shuffle spills to disk, straggler tasks, and catastrophic executor out-of-memory errors.

## 3. Concept Diagram
```
  Broadcast Hash Join (BHJ - Fast, No Shuffle):
    Table A (Large) ───────────► [ Hash Match ] ──► Joined Output
    Table B (Small <= 10MB) ───► Broadcasted to all Executors

  Sort Merge Join (SMJ - Scalable, Full Shuffle):
    Table A ──► [ Shuffle by Key ] ──► [ Sort Key ] ──┐
                                                      ├──► [ Merge ] ──► Output
    Table B ──► [ Shuffle by Key ] ──► [ Sort Key ] ──┘
```

## 7. Common Mistakes & Gotchas
- **Broadcasting tables that exceed driver memory**: The broadcast hint copies the entire table to the driver JVM before broadcasting. Broadcasting a multi-gigabyte table causes driver OOM.
- **Accidental Cartesian product (Cross Join)**: Omitting join conditions triggers `AnalysisException: Detected implicit cartesian product...`. If intentional, you must call `crossJoin()`.
- **Duplicate key explosions**: Joining tables with non-unique keys multiplies row counts exponentially, causing massive memory spikes.

## 8. Official Documentation
- Spark Join Hints Guide: https://spark.apache.org/docs/latest/sql-performance-tuning.html#join-strategy-hints-for-sql-queries
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
from pyspark.sql.functions import broadcast, col

def run_module_08():
    print("=================================================================")
    print(" Module 08: Join Types, Physical Strategies & Plan Inspection    ")
    print("=================================================================")

    spark = get_spark_session("Module08-JoinsAndStrategies")

    df_orders = spark.read.json("data/raw/orders.json")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")
    df_products = spark.read.parquet("data/raw/products.parquet")
    df_items = spark.read.option("header", "true").csv("data/raw/order_items.csv")

    # 1. Join Types Exploration (Inner, Left, Semi, Anti)
    print("\n[Step 1] Demonstrating Diverse Join Types...")
    
    # Left Semi Join (Returns only columns from left table where a match exists in right)
    df_semi = df_customers.join(df_orders, on="customer_id", how="left_semi")
    print(f"[✓] Left Semi (Customers with at least 1 order): {df_semi.count()} customers")

    # Left Anti Join (Returns left rows with NO match in right table)
    df_anti = df_customers.join(df_orders, on="customer_id", how="left_anti")
    print(f"[✓] Left Anti (Customers with 0 orders): {df_anti.count()} customers")

    # Full Outer Join
    df_full = df_customers.join(df_orders.limit(10), on="customer_id", how="full")
    print(f"[✓] Full Outer Join sample count: {df_full.count()}")

    # 2. Join Strategies: Broadcast Hash Join vs Sort-Merge Join
    print("\n[Step 2] Comparing Physical Execution Plans: Broadcast vs Sort-Merge...")
    
    # A) Broadcast Hash Join (Forcing broadcast with hint)
    print("\n--- Plan A: Broadcast Hash Join [broadcast(df_products)] ---")
    df_bhj = df_items.join(broadcast(df_products), on="product_id", how="inner")
    df_bhj.explain()

    # B) Sort-Merge Join (Disabling broadcast join threshold)
    print("\n--- Plan B: Sort-Merge Join [spark.sql.autoBroadcastJoinThreshold = -1] ---")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    df_smj = df_items.join(df_products, on="product_id", how="inner")
    df_smj.explain()
    
    # Restore threshold to default (10MB)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

    # 3. Cross Join (Cartesian Product)
    print("\n[Step 3] Explicit Cross Join...")
    sample_categories = df_products.select("category").distinct().limit(3)
    sample_tiers = df_customers.select("loyalty_tier").distinct().limit(2)
    cross_df = sample_categories.crossJoin(sample_tiers)
    print(f"[✓] Cross join produced {cross_df.count()} combinations:")
    cross_df.show()

    print("\n[*] Module 08 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_08()

"""
## 5. Expected Output
=================================================================
 Module 08: Join Types, Physical Strategies & Plan Inspection    
=================================================================

[Step 1] Demonstrating Diverse Join Types...
[✓] Left Semi (Customers with at least 1 order): ... customers
[✓] Left Anti (Customers with 0 orders): ... customers
[✓] Full Outer Join sample count: ...

[Step 2] Comparing Physical Execution Plans: Broadcast vs Sort-Merge...

--- Plan A: Broadcast Hash Join [broadcast(df_products)] ---
== Physical Plan ==
AdaptiveSparkPlan ...
+- BroadcastHashJoin [product_id#...], [product_id#...], Inner, BuildRight ...

--- Plan B: Sort-Merge Join [spark.sql.autoBroadcastJoinThreshold = -1] ---
== Physical Plan ==
AdaptiveSparkPlan ...
+- SortMergeJoin [product_id#...], [product_id#...], Inner ...

[Step 3] Explicit Cross Join...
[✓] Cross join produced 6 combinations:
+-----------+------------+
|   category|loyalty_tier|
+-----------+------------+
|Electronics|      Bronze|
|Electronics|      Silver|
...

[*] Module 08 completed successfully.

## 6. Try It Yourself (Exercises)
1. What error message does Spark throw if you write `df_a.join(df_b)` without a join condition when `spark.sql.crossJoin.enabled=false`?
2. Compare the runtime of joining `df_orders` with `df_customers` using broadcast vs without broadcast.
3. How does Spark 3.x Adaptive Query Execution (AQE) dynamically convert a Sort-Merge Join to a Broadcast Hash Join at runtime?
"""

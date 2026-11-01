"""
# Module 01: Apache Spark Architecture & DAG Execution

## 1. What It Is
Apache Spark is a distributed computing engine that coordinates clusters of computers to process large datasets in parallel.
It uses a Master/Worker model where a central **Driver** program breaks down computational queries into a Directed Acyclic Graph (DAG) of **Stages** and **Tasks**, which are executed by worker **Executors**.

## 2. Why It Matters
Understanding the architecture is critical for debugging performance issues, out-of-memory errors, and shuffle bottlenecks.
Without knowing how Spark divides work into narrow vs wide dependencies, developers often write code that triggers massive data movement across the network.

## 3. Concept Diagram
```
                     +---------------------------------------+
                     |             Spark Driver              |
                     |  (SparkSession, DAGScheduler, TaskUI) |
                     +-------------------+-------------------+
                                         |
                     +-------------------+-------------------+
                     |          Cluster Manager              |
                     |   (Standalone / YARN / K8s / Local)   |
                     +---------+-------------------+---------+
                               |                   |
            +------------------+                   +------------------+
            |                                                         |
  +---------v-----------+                                   +---------v-----------+
  |  Worker Node 1      |                                   |  Worker Node 2      |
  |  +---------------+  |                                   |  +---------------+  |
  |  |  Executor 1   |  |                                   |  |  Executor 2   |  |
  |  | [Task][Task]  |  |                                   |  | [Task][Task]  |  |
  |  | BlockManager  |  |                                   |  | BlockManager  |  |
  |  +---------------+  |                                   |  +---------------+  |
  +---------------------+                                   +---------------------+
```

## 7. Common Mistakes & Gotchas
- **Calling actions inside loops**: Invoking `.count()`, `.collect()`, or `.show()` repeatedly forces Spark to trigger separate DAGs for every iteration.
- **Collecting large datasets**: Calling `.collect()` brings all partition data into the driver JVM memory, causing `OutOfMemoryError: Java heap space`.
- **Confusing Transformations with Actions**: Forgetting that transformations like `.map()`, `.filter()`, and `.groupBy()` do nothing until an action (e.g. `.count()`, `.write`) is called.

## 8. Official Documentation
- Cluster Mode Overview: https://spark.apache.org/docs/latest/cluster-overview.html
- Spark Architecture & Tuning: https://spark.apache.org/docs/latest/tuning.html
"""

import sys
import os
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from scripts.spark_session import get_spark_session

def run_module_01():
    print("=================================================================")
    print(" Module 01: Spark Architecture, DAG, Lazy Evaluation & Actions  ")
    print("=================================================================")

    # 1. Initialize SparkSession (The entry point and Driver controller)
    spark = get_spark_session("Module01-SparkArchitecture")
    sc = spark.sparkContext

    print(f"\n[*] Driver Application ID: {sc.applicationId}")
    print(f"[*] Spark Master URL:      {sc.master}")
    print(f"[*] Spark Version:         {spark.version}")
    print(f"[*] Default Parallelism:   {sc.defaultParallelism}")
    print(f"[*] Spark UI URL:          {spark.sparkContext.uiWebUrl}")

    # 2. Demonstrate Lazy Evaluation vs Actions
    print("\n[Step 1] Defining Transformations (LAZY EVALUATION)...")
    raw_customers_path = "data/raw/customers.csv"

    # Transformation: read (defines schema and logical plan, but does not read all data)
    df_raw = spark.read.option("header", "true").csv(raw_customers_path)
    
    # Transformation: filter (creates an Execution DAG node)
    df_filtered = df_raw.filter(df_raw.loyalty_tier == "Gold")
    
    # Transformation: select projection
    df_selected = df_filtered.select("customer_id", "first_name", "last_name", "loyalty_tier")

    print("[*] Transformations defined. Notice: No Spark jobs or tasks have executed yet!")
    print(f"[*] Execution Plan:\n")
    df_selected.explain(mode="simple")

    # 3. Triggering Actions (Executes the DAG of Stages and Tasks)
    print("\n[Step 2] Executing Action 1: count()...")
    start_time = time.time()
    gold_count = df_selected.count()
    duration = time.time() - start_time
    print(f"[✓] Found {gold_count} Gold loyalty customers in {duration:.3f}s")

    print("\n[Step 3] Executing Action 2: take(5)...")
    sample_records = df_selected.take(5)
    print(f"[✓] First 5 Gold customers retrieved to driver:")
    for row in sample_records:
        print(f"    - ID: {row.customer_id}, Name: {row.first_name} {row.last_name}, Tier: {row.loyalty_tier}")

    # 4. Inspecting the Stages and RDD Lineage (DAG)
    print("\n[Step 4] Printing RDD Lineage (toDebugString):")
    rdd_lineage = df_selected.rdd.toDebugString().decode("utf-8")
    for line in rdd_lineage.splitlines():
        print(f"    | {line}")

    # Keep session briefly if interactive
    print("\n[*] Module 01 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_01()

"""
## 5. Expected Output
=================================================================
 Module 01: Spark Architecture, DAG, Lazy Evaluation & Actions  
=================================================================

[*] Driver Application ID: local-17...
[*] Spark Master URL:      local[*]
[*] Spark Version:         3.5.x
[*] Default Parallelism:   ...
[*] Spark UI URL:          http://...

[Step 1] Defining Transformations (LAZY EVALUATION)...
[*] Transformations defined. Notice: No Spark jobs or tasks have executed yet!
[*] Execution Plan:
== Physical Plan ==
*(1) Project [customer_id#..., first_name#..., last_name#..., loyalty_tier#...]
+- *(1) Filter (isnotnull(loyalty_tier#...) AND (loyalty_tier#... = Gold))
   +- FileScan csv [customer_id#...,first_name#...,last_name#...,loyalty_tier#...] ...

[Step 2] Executing Action 1: count()...
[✓] Found 146 Gold loyalty customers in 0.320s

[Step 3] Executing Action 2: take(5)...
[✓] First 5 Gold customers retrieved to driver:
    - ID: ..., Name: ... ..., Tier: Gold
    ...

[Step 4] Printing RDD Lineage (toDebugString):
    | (1) MapPartitionsRDD[12] at ...
    | ...

[*] Module 01 completed successfully.

## 6. Try It Yourself (Exercises)
1. Add a second filter transformation for customers whose `state == 'CA'`. Inspect the physical plan using `.explain()`. Did Catalyst combine the two filters into a single stage?
2. Call `.rdd.getNumPartitions()` on `df_selected`. How does Spark decide the default number of partitions when reading a CSV file?
3. Replace `.take(5)` with `.collect()`. What is the fundamental difference in network transmission and driver memory between the two?
"""

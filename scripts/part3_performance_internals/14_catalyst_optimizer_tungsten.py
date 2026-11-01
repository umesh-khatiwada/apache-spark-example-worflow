"""
# Module 14: Catalyst Optimizer & Tungsten Engine Internals

## 1. What It Is
The **Catalyst Optimizer** is Spark's extensible relational query optimization framework that transforms high-level DataFrame/SQL queries into highly optimized physical execution trees.
The **Tungsten Engine** provides hardware-level CPU and memory optimizations, using off-heap binary memory management and **Whole-Stage Code Generation (WSCG)** to collapse multiple operators into a single tight Java bytecode loop.

## 2. Why It Matters
Understanding Catalyst's 4 stages (Analysis, Logical Optimization, Physical Planning, and Code Generation) is the key to reading query execution plans.
It allows engineers to verify whether predicates were pushed down to storage, whether unused columns were pruned, and why certain queries run orders of magnitude faster.

## 3. Concept Diagram
```
  SQL / DataFrame Query
          │
          ▼
  1. Unresolved Logical Plan  ──► [ Catalog / Metastore ]
          │
          ▼
  2. Analyzed Logical Plan    ──► [ Rule-Based Optimizer (RBO) ]
          │                          • Predicate Pushdown
          │                          • Projection Pruning
          ▼                          • Constant Folding
  3. Optimized Logical Plan   ──► [ Cost-Based Optimizer (CBO) ]
          │
          ▼
  4. Physical Plans           ──► Select Lowest Cost Physical Strategy
          │
          ▼
  5. Whole-Stage Code Gen     ──► Generates JVM Bytecode (* in plan)
```

## 7. Common Mistakes & Gotchas
- **Misinterpreting the asterisk `*(1)` in explain plans**: The asterisk indicates that the operator is part of a Whole-Stage Code Generation pipeline. Missing asterisks mean the engine fell back to interpreted volcano-iterator mode (slower).
- **Breaking predicate pushdown with custom UDFs**: Applying a Python UDF in a filter prevents Catalyst from pushing the filter down into the Parquet/ORC file reader. Use built-in column expressions so storage scan can skip row groups.
- **Ignoring filter order**: While Catalyst pushes down simple filters automatically, complex subqueries or non-deterministic operations may block pushdown if not structured cleanly.

## 8. Official Documentation
- Catalyst Optimizer Architecture: https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html
- Explain Plan Documentation: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html
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
from pyspark.sql.functions import col, lit

def run_module_14():
    print("=================================================================")
    print(" Module 14: Catalyst Optimizer & Tungsten Engine Internals       ")
    print("=================================================================")

    spark = get_spark_session("Module14-CatalystTungsten")

    # Ingest Parquet dataset
    df_products = spark.read.parquet("data/raw/products.parquet")

    # Construct a query with opportunities for Catalyst optimization:
    # 1. Constant folding: 100.0 * 1.05 -> precomputed constant 105.0
    # 2. Projection pruning: Reading only price, category, name (stock_quantity and supplier_id pruned)
    # 3. Predicate pushdown: Filtering price > 105.0 directly inside the Parquet scan!
    query_df = (
        df_products
        .filter(col("price") > lit(100.0) * lit(1.05))
        .filter(col("category") == "Electronics")
        .select("product_id", "name", "price")
    )

    # 1. Inspecting the 4 Catalyst Stages with explain(mode="extended")
    print("\n[Step 1] Inspecting the 4 Catalyst Optimizer Stages (mode='extended')...")
    query_df.explain(mode="extended")

    # 2. Formatted Plan with operator breakdowns (mode="formatted")
    print("\n[Step 2] Formatted Execution Plan (mode='formatted'):")
    query_df.explain(mode="formatted")

    # 3. Verifying Predicate and Projection Pushdown in the Physical Plan
    print("\n[Step 3] Verifying Physical Storage Pushdown:")
    physical_plan_str = query_df._jdf.queryExecution().executedPlan().toString()
    
    if "PushedFilters" in physical_plan_str:
        print("[✓] PUSHED FILTERS DETECTED: Filters were pushed directly into the Parquet storage reader!")
    if "ReadSchema: struct<product_id" in physical_plan_str:
        print("[✓] PROJECTION PRUNING DETECTED: Storage reader read ONLY requested columns, ignoring unused fields!")
    if "*(" in physical_plan_str:
        print("[✓] WHOLE-STAGE CODE GENERATION ACTIVE: Operators are compiled together into JVM bytecode loops!")

    print("\n[*] Module 14 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_14()

"""
## 5. Expected Output
=================================================================
 Module 14: Catalyst Optimizer & Tungsten Engine Internals       
=================================================================

[Step 1] Inspecting the 4 Catalyst Optimizer Stages (mode='extended')...
== Parsed Logical Plan ==
...
== Analyzed Logical Plan ==
...
== Optimized Logical Plan ==
...
== Physical Plan ==
*(1) Project [product_id#...L, name#..., price#...]
+- *(1) Filter (((isnotnull(price#...) AND isnotnull(category#...)) AND (price#... > 105.0)) AND (category#... = Electronics))
   +- FileScan parquet [...] Batched: true, PushedFilters: [IsNotNull(price), IsNotNull(category), GreaterThan(price,105.0), EqualTo(category,Electronics)], ReadSchema: struct<product_id:bigint,name:string,category:string,price:double>

[Step 2] Formatted Execution Plan (mode='formatted'):
...

[Step 3] Verifying Physical Storage Pushdown:
[✓] PUSHED FILTERS DETECTED: Filters were pushed directly into the Parquet storage reader!
[✓] PROJECTION PRUNING DETECTED: Storage reader read ONLY requested columns, ignoring unused fields!
[✓] WHOLE-STAGE CODE GENERATION ACTIVE: Operators are compiled together into JVM bytecode loops!

[*] Module 14 completed successfully.

## 6. Try It Yourself (Exercises)
1. Add a non-deterministic condition `filter(rand() > 0.5)`. Does Catalyst push `rand()` down to the storage scan? Why or why not?
2. Compare the output of `explain(mode="cost")` with and without table statistics generated by `ANALYZE TABLE COMPUTE STATISTICS`.
3. Locate the `WholestageCodegen` sub-tree in the formatted plan and count how many physical operators were fused into stage 1.
"""

"""
# Module 34: Spark Declarative Pipelines & Data Quality Expectations

## 1. What It Is
**Declarative Pipelines** (exemplified by Delta Live Tables and Spark Pipeline frameworks) allow data engineers to define data transformations declaratively as a directed acyclic graph of tables.
Instead of writing imperative plumbing code (`df.write.save()`, manage transaction checkpoints, handle failure retries), you declare target table schemas, input dependencies, and **Data Quality Expectations**.

## 2. Why It Matters
Imperative ETL pipelines are brittle: a single pipeline script mixes business transformation logic, connection retry loops, and schema tracking.
Declarative pipelines separate *what* data to produce from *how* to orchestrate and optimize it, enabling automated recovery, data lineage graphs, and automated data quality validation.

## 3. Concept Diagram
```
  Declarative Pipeline Graph:
  [Raw Orders Ingestion] ──► EXPECT (total_amount > 0) ON VIOLATION DROP ROW
             │
             ▼
  [Silver Orders Table]  ──► EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL
             │
             ▼
  [Gold Customer Lifetime Value] (Materialized Mart)
```

## 7. Common Mistakes & Gotchas
- **Mixing imperative side-effects with declarative transformations**: Calling `.collect()` or writing to external files inside a declarative pipeline definition breaks execution graph generation.
- **Overly strict expectations causing total pipeline failure**: Using `ON VIOLATION FAIL` for minor cosmetic validation errors halts entire production workflows. Reserve `FAIL` for critical primary keys and use `DROP ROW` or `WARN` for dirty telemetry fields.
- **Circular dependencies**: Defining Table A that depends on Table B, which in turn references Table A, creates an unresolvable cyclic DAG error.

## 8. Official Documentation
- Delta Live Tables Overview: https://docs.databricks.com/delta-live-tables/index.html
- Spark Declarative Data Quality & Governance: https://spark.apache.org/docs/latest/
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
from pyspark.sql.functions import col, when, count, expr

# Declarative Pipeline Framework Implementation in Python
class DeclarativeTable:
    def __init__(self, name: str, query_fn, expectations: list = None):
        self.name = name
        self.query_fn = query_fn
        self.expectations = expectations or [] # Tuple of (constraint_expr, on_violation: "DROP" | "FAIL" | "WARN")

class DeclarativePipelineRunner:
    def __init__(self, spark, warehouse_dir: str):
        self.spark = spark
        self.warehouse_dir = warehouse_dir
        self.tables = {}

    def register(self, table: DeclarativeTable):
        self.tables[table.name] = table

    def run(self):
        print(f"[*] Executing Declarative Pipeline DAG across {len(self.tables)} declared tables...")
        for name, tbl in self.tables.items():
            print(f"\n--- Materializing Table: '{name}' ---")
            df = tbl.query_fn(self.spark)
            
            # Apply Data Quality Expectations
            for constraint, on_violation in tbl.expectations:
                total_before = df.count()
                valid_df = df.filter(expr(constraint))
                valid_count = valid_df.count()
                violations = total_before - valid_count
                
                print(f"    - Expectation: [{constraint}] -> Violations: {violations} of {total_before} rows")
                if violations > 0:
                    if on_violation == "FAIL":
                        raise ValueError(f"[CRITICAL QUALITY FAILURE] Table '{name}' violated rule: {constraint}")
                    elif on_violation == "DROP":
                        print(f"      [ACTION: DROP ROW] Dropped {violations} non-compliant rows.")
                        df = valid_df
                    elif on_violation == "WARN":
                        print(f"      [ACTION: WARN] Metric flagged for data quality dashboard.")
            
            # Save materialized table
            target_path = os.path.join(self.warehouse_dir, name)
            df.write.mode("overwrite").parquet(target_path)
            print(f"[✓] Successfully materialized {df.count()} clean rows to '{target_path}'")

def run_module_34():
    print("=================================================================")
    print(" Module 34: Spark Declarative Pipelines & Data Quality Rules     ")
    print("=================================================================")

    spark = get_spark_session("Module34-DeclarativePipelines")
    warehouse = "data/warehouse/module34_declarative_pipeline"
    if os.path.exists(warehouse):
        shutil.rmtree(warehouse)
    os.makedirs(warehouse, exist_ok=True)

    # 1. Define Declarative Tables with Quality Expectations
    
    # Table 1: Bronze Orders (Ingestion with Drop Row expectation)
    bronze_orders = DeclarativeTable(
        name="bronze_orders",
        query_fn=lambda spk: spk.read.json("data/raw/orders.json"),
        expectations=[
            ("total_amount IS NOT NULL AND total_amount > 0", "DROP"),
            ("customer_id IS NOT NULL", "FAIL")
        ]
    )

    # Table 2: Silver High-Value Orders
    silver_high_value = DeclarativeTable(
        name="silver_high_value_orders",
        query_fn=lambda spk: spk.read.parquet(f"{warehouse}/bronze_orders").filter(col("total_amount") > 150.0),
        expectations=[
            ("status IN ('COMPLETED', 'SHIPPED', 'PENDING')", "WARN")
        ]
    )

    # 2. Execute Declarative Runner
    runner = DeclarativePipelineRunner(spark, warehouse)
    runner.register(bronze_orders)
    runner.register(silver_high_value)
    runner.run()

    # 3. Inspect Materialized Output
    print("\n[Step 3] Final Materialized Silver Table Sample:")
    spark.read.parquet(f"{warehouse}/silver_high_value_orders").select("order_id", "customer_id", "status", "total_amount").show(5)

    print("\n[*] Module 34 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_34()

"""
## 5. Expected Output
=================================================================
 Module 34: Spark Declarative Pipelines & Data Quality Rules     
=================================================================

[*] Executing Declarative Pipeline DAG across 2 declared tables...

--- Materializing Table: 'bronze_orders' ---
    - Expectation: [total_amount IS NOT NULL AND total_amount > 0] -> Violations: 0 of 5000 rows
    - Expectation: [customer_id IS NOT NULL] -> Violations: 0 of 5000 rows
[✓] Successfully materialized 5000 clean rows to ...

--- Materializing Table: 'silver_high_value_orders' ---
    - Expectation: [status IN ('COMPLETED', 'SHIPPED', 'PENDING')] -> Violations: ... of ... rows
      [ACTION: WARN] Metric flagged for data quality dashboard.
[✓] Successfully materialized ... clean rows to ...

[*] Module 34 completed successfully.

## 6. Try It Yourself (Exercises)
1. Add an expectation `("email LIKE '%@%'", "DROP")` to a customer ingestion table.
2. Simulate a critical pipeline halt by adding an expectation `("total_amount < 100", "FAIL")`. Observe the exception trace.
3. How does Delta Live Tables (DLT) maintain change-data-capture (CDC) with `APPLY CHANGES INTO`?
"""

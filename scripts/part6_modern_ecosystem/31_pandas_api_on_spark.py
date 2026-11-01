"""
# Module 31: pandas API on Spark (pyspark.pandas / Koalas)

## 1. What It Is
The **pandas API on Spark** (formerly Koalas, integrated directly into PySpark as `pyspark.pandas` in Spark 3.2+) provides a drop-in pandas-compatible interface on top of Spark DataFrames.
It allows data scientists familiar with pandas syntax (`df.groupby().sum()`, `df.plot()`, `df.describe()`) to scale their workloads across thousands of cores without rewriting code in PySpark SQL.

## 2. Why It Matters
Standard pandas is single-threaded and bound to the RAM of a single machine (typically failing on datasets > 5GB).
`pyspark.pandas` translates familiar pandas operations directly into Catalyst logical plans, executing transparently across cluster nodes with distributed parallel speed.

## 3. Concept Diagram
```
  Pandas Code:                                  Spark Execution:
  import pyspark.pandas as ps
  psdf = ps.read_csv("orders.csv")     ──►     Distributed DataFrame
  psdf[psdf["amount"] > 100].describe() ──►     Catalyst Query Optimizer
                                                       │
                                                       ▼
                                            Parallel Tungsten Tasks
```

## 7. Common Mistakes & Gotchas
- **Default Index Shuffle Overhead**: Pandas relies on strict row indexing (`index`). In distributed Spark, maintaining exact sequential row ordering requires a global shuffle (`compute.default_index_type = "sequence"`). Use `"distributed-sequence"` or `"distributed"` index to eliminate shuffle bottlenecks.
- **Calling `.to_pandas()` unnecessarily**: Calling `.to_pandas()` pulls the entire distributed dataset into driver RAM, completely defeating the purpose of distributed computing.
- **Operations that require full data collection**: Functions like `psdf.quantile()` or sorting on non-indexed columns may require full cluster shuffles.

## 8. Official Documentation
- pandas API on Spark Guide: https://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html
"""

import sys
import os

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
# Project root setup (compatible with both standalone execution and Jupyter notebooks)
if "__file__" in locals():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
else:
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../..") if "notebooks" in os.getcwd() else os.getcwd())
    os.chdir(project_root)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
from scripts.spark_session import get_spark_session
import pyspark.pandas as ps

def run_module_31():
    print("=================================================================")
    print(" Module 31: pandas API on Spark (Migrating Pandas Code at Scale)  ")
    print("=================================================================")

    # Initialize SparkSession first
    spark = get_spark_session("Module31-PandasOnSpark")

    # Configure optimal distributed index type (avoids expensive single-partition indexing)
    ps.set_option("compute.default_index_type", "distributed")

    # 1. Loading Data with pandas Syntax
    print("\n[Step 1] Loading CSV directly into a pandas-on-Spark DataFrame...")
    psdf_customers = ps.read_csv("data/raw/customers.csv")
    print(f"[*] Type of psdf: {type(psdf_customers)}")
    print(f"[*] Dataset Shape: {psdf_customers.shape}")
    print("[✓] First 3 rows via .head():")
    print(psdf_customers.head(3))

    # 2. Familiar Pandas Operations at Scale
    print("\n[Step 2] Applying Native Pandas Aggregations & Grouping...")
    # Group by loyalty tier and compute summary statistics
    tier_counts = psdf_customers["loyalty_tier"].value_counts()
    print("[✓] Value Counts (loyalty_tier):")
    print(tier_counts)

    # 3. Interoperability: Converting between PySpark DataFrame & pandas-on-Spark
    print("\n[Step 3] Seamless Interoperability (PySpark DataFrame <-> pandas-on-Spark)...")
    pyspark_orders = spark.read.json("data/raw/orders.json")
    
    # Convert PySpark DataFrame to pandas-on-Spark (Zero-copy logical conversion)
    psdf_orders = pyspark_orders.pandas_api()
    print(f"[✓] Converted PySpark DF -> pandas-on-Spark DF: Shape {psdf_orders.shape}")

    # Calculate payment method summary using supported pandas syntax
    summary_df = psdf_orders[["payment_method", "total_amount"]].groupby("payment_method").mean()
    print("[✓] GroupBy mean spend per payment method across Spark cluster:")
    print(summary_df.round(2))

    # Convert back to native PySpark DataFrame
    back_to_pyspark = psdf_orders.to_spark()
    print(f"[✓] Converted back to native PySpark DataFrame: Schema has {len(back_to_pyspark.columns)} columns.")

    print("\n[*] Module 31 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_31()

"""
## 5. Expected Output
=================================================================
 Module 31: pandas API on Spark (Migrating Pandas Code at Scale)  
=================================================================

[Step 1] Loading CSV directly into a pandas-on-Spark DataFrame...
[*] Type of psdf: <class 'pyspark.pandas.frame.DataFrame'>
[*] Dataset Shape: (1000, 8)
...

[Step 2] Applying Native Pandas Aggregations & Grouping...
[✓] Value Counts (loyalty_tier):
Bronze      486
Silver      330
Gold        140
Platinum     44
Name: count, dtype: int64

[Step 3] Seamless Interoperability (PySpark DataFrame <-> pandas-on-Spark)...
[✓] Converted PySpark DF -> pandas-on-Spark DF: Shape (5000, 7)
...

[*] Module 31 completed successfully.

## 6. Try It Yourself (Exercises)
1. Benchmark `.describe()` using `pyspark.pandas` vs native Python pandas on a 10x scaled dataset.
2. What are the differences between index types: `"sequence"`, `"distributed-sequence"`, and `"distributed"`?
3. Use `psdf.transform_batch()` to apply arbitrary custom functions across distributed batches.
"""

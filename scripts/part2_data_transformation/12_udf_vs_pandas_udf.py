"""
# Module 12: User-Defined Functions (Python UDF vs Pandas UDF vs Built-in)

## 1. What It Is
User-Defined Functions (UDFs) extend Spark SQL with custom user logic when no built-in function satisfies requirements.
Spark provides two main styles:
- **Standard Python UDF**: Processes records row-by-row, serializing each Python object across JVM-Python IPC sockets.
- **Pandas UDF (Vectorized with Apache Arrow)**: Transfers columnar Arrow record batches between JVM and Python workers, enabling SIMD and vectorized Pandas/NumPy execution.

## 2. Why It Matters
Standard Python UDFs represent the single biggest performance killer in PySpark pipelines.
Vectorized Pandas UDFs transfer entire blocks in C++ memory via Apache Arrow, running 5x to 20x faster, while native built-in functions remain 50x faster.

## 3. Concept Diagram
```
  1. Standard Python UDF (Row-by-Row):
     JVM Row ──(pickle)──► IPC Pipe ──(unpickle)──► Python Worker (GIL) ──(pickle)──► JVM
     * Huge serialization overhead, CPU bound, slow *

  2. Pandas UDF with Apache Arrow (Vectorized Batch):
     JVM Columns ──(Zero-copy Arrow RecordBatch)──► Python C++ / Pandas Vector ──► JVM
     * High throughput, vectorized execution *

  3. Built-in Function (pyspark.sql.functions):
     Direct JVM / Tungsten bytecode execution (Zero Python involvement!)
```

## 7. Common Mistakes & Gotchas
- **Forgetting Arrow configuration**: Ensure `spark.sql.execution.arrow.pyspark.enabled` is `true` (default in modern Spark) to unlock vectorized speeds.
- **Arrow memory pressure**: Vectorized Pandas UDFs load chunks into worker memory. If `spark.sql.execution.arrow.maxRecordsPerBatch` (default 10,000) is too large for complex objects, worker OOMs occur.
- **Non-deterministic UDF outputs**: UDFs that call `random()` or `time.time()` will produce inconsistent results if Spark retries a failed stage task. Mark non-deterministic UDFs explicitly with `.asNondeterministic()`.

## 8. Official Documentation
- PySpark Pandas UDFs: https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html
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
from pyspark.sql.functions import col, udf, pandas_udf, round as _round
from pyspark.sql.types import DoubleType
import pandas as pd

# 1. Define Standard Python UDF (Row-by-Row)
@udf(returnType=DoubleType())
def standard_python_tax_calc(amount: float) -> float:
    if amount is None:
        return 0.0
    # Simulate custom business logic: progressive tax bracket
    if amount > 1000.0:
        return round(amount * 0.18, 2)
    elif amount > 200.0:
        return round(amount * 0.12, 2)
    return round(amount * 0.05, 2)

# 2. Define Vectorized Pandas UDF (Apache Arrow)
@pandas_udf(DoubleType())
def vectorized_pandas_tax_calc(amounts: pd.Series) -> pd.Series:
    # Vectorized calculation on entire Pandas Series in C-memory
    tax = pd.Series(0.0, index=amounts.index)
    tax = tax.where(amounts <= 200.0, amounts * 0.12)
    tax = tax.where(amounts <= 1000.0, amounts * 0.18)
    tax = tax.where(amounts > 200.0, amounts * 0.05)
    return tax.round(2)

def run_module_12():
    print("=================================================================")
    print(" Module 12: UDFs (Python UDF vs Pandas UDF vs Built-in)          ")
    print("=================================================================")

    spark = get_spark_session("Module12-UDFBenchmark")

    # Generate a synthetic benchmark dataset with 200,000 rows
    print("\n[Step 1] Preparing 200,000 synthetic transaction records for benchmark...")
    df_bench = spark.range(0, 200000, 1, numPartitions=4).select(
        (col("id") * 0.15).alias("amount")
    )
    df_bench.cache().count() # Materialize in memory so disk I/O doesn't skew benchmark

    # Benchmark 1: Standard Python UDF (Pickle serialization)
    print("\n[Step 2] Executing Standard Python UDF (Row-by-Row)...")
    t0 = time.time()
    res_py = df_bench.withColumn("tax", standard_python_tax_calc(col("amount"))).agg({"tax": "sum"}).collect()
    py_time = time.time() - t0
    print(f"[✓] Python UDF Sum: {res_py[0][0]:.2f} | Duration: {py_time:.4f}s")

    # Benchmark 2: Vectorized Pandas UDF (Apache Arrow)
    print("\n[Step 3] Executing Vectorized Pandas UDF (Apache Arrow)...")
    t0 = time.time()
    res_pandas = df_bench.withColumn("tax", vectorized_pandas_tax_calc(col("amount"))).agg({"tax": "sum"}).collect()
    pandas_time = time.time() - t0
    print(f"[✓] Pandas UDF Sum: {res_pandas[0][0]:.2f} | Duration: {pandas_time:.4f}s")

    # Benchmark 3: Native Built-in Expressions (Tungsten JVM Bytecode)
    print("\n[Step 4] Executing Native Built-in Spark SQL Expressions...")
    from pyspark.sql.functions import when
    t0 = time.time()
    native_expr = (
        when(col("amount") > 1000.0, _round(col("amount") * 0.18, 2))
        .when(col("amount") > 200.0, _round(col("amount") * 0.12, 2))
        .otherwise(_round(col("amount") * 0.05, 2))
    )
    res_native = df_bench.withColumn("tax", native_expr).agg({"tax": "sum"}).collect()
    native_time = time.time() - t0
    print(f"[✓] Native Built-in Sum: {res_native[0][0]:.2f} | Duration: {native_time:.4f}s")

    # Speedup Summary
    print("\n================ BENCHMARK SUMMARY ================")
    print(f" Python UDF:       {py_time:.4f}s (Baseline 1.0x)")
    print(f" Pandas Arrow UDF: {pandas_time:.4f}s ({py_time / max(pandas_time, 0.0001):.1f}x Faster than Python UDF)")
    print(f" Native Built-in:  {native_time:.4f}s ({py_time / max(native_time, 0.0001):.1f}x Faster than Python UDF)")
    print("====================================================")

    print("\n[*] Module 12 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_12()

"""
## 5. Expected Output
=================================================================
 Module 12: UDFs (Python UDF vs Pandas UDF vs Built-in)          
=================================================================

[Step 1] Preparing 200,000 synthetic transaction records for benchmark...
[Step 2] Executing Standard Python UDF (Row-by-Row)...
[✓] Python UDF Sum: ... | Duration: 1.2500s
[Step 3] Executing Vectorized Pandas UDF (Apache Arrow)...
[✓] Pandas UDF Sum: ... | Duration: 0.3500s
[Step 4] Executing Native Built-in Spark SQL Expressions...
[✓] Native Built-in Sum: ... | Duration: 0.0800s

================ BENCHMARK SUMMARY ================
 Python UDF:       ...s (Baseline 1.0x)
 Pandas Arrow UDF: ...s (...x Faster than Python UDF)
 Native Built-in:  ...s (...x Faster than Python UDF)
====================================================

[*] Module 12 completed successfully.

## 6. Try It Yourself (Exercises)
1. Write an iterator-to-iterator Pandas UDF (`pandas_udf("double", PandasUDFType.SCALAR_ITER)`) to initialize state once per partition.
2. What happens if a Pandas UDF returns a Series with a different length than the input Series?
3. Register `standard_python_tax_calc` into Spark SQL via `spark.udf.register()` and run it inside a SQL query.
"""

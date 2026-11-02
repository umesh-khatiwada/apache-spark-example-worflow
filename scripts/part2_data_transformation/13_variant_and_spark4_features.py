"""
# Module 13: Modern Spark SQL Features (VARIANT Type, SQL Scripting, Safe Functions)

## 1. What It Is
Modern Apache Spark (Spark 3.5 and Spark 4.0) introduced major innovations in semi-structured data and SQL expressiveness:
- **VARIANT Data Type**: A native binary encoding for semi-structured data that avoids schema rigidity while outperforming raw JSON strings through sub-column shredding.
- **Resilient ANSI Expressions**: Functions like `try_cast()`, `try_divide()`, `try_multiply()` return `NULL` on overflow or format errors rather than failing jobs.
- **SQL Scripting & Variables**: Spark 4.x allows multi-statement procedural SQL scripts with `DECLARE`, `SET`, `IF`, and loops.

## 2. Why It Matters
Pipelines frequently ingest schema-drifting JSON payloads. In older Spark, developers had to choose between slow JSON string parsing or rigid schemas.
The `VARIANT` type provides flexible semi-structured querying with columnar performance. `try_cast()` ensures pipelines never fail on dirty data in strict ANSI mode.

## 3. Concept Diagram
```
  Evolution of Semi-Structured Storage in Spark:
  1. String JSON: spark.read.json(...)  --> Full string parse on every access (Slow)
  2. Struct/Array Schema: StructType    --> Fast, but fails on schema drift or unknown fields
  3. VARIANT Type (Spark 4.0 / 3.5+):   --> Binary, flexible, supports path extraction & shredding!
```

## 7. Common Mistakes & Gotchas
- **VARIANT compatibility across table formats**: Early versions of Parquet and Delta Lake readers require specific reader versions to parse the physical VARIANT representation.
- **Relying on try_cast() when ANSI mode is off**: When `spark.sql.ansi.enabled` is `false` (default in Spark 3.x), standard `cast()` already returns `NULL` on error. In Spark 4.0 (where ANSI mode is default), `try_cast()` is essential to avoid runtime errors.

## 8. Official Documentation
- Spark 4.0 / 3.5 Release Notes & VARIANT Type: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
- ANSI Compliance Guide: https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html
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
from pyspark.sql.functions import col, expr, lit, coalesce

def run_module_13():
    print("=================================================================")
    print(" Module 13: Modern Spark SQL Features (VARIANT, Safe Expressions)")
    print("=================================================================")

    spark = get_spark_session("Module13-ModernSparkFeatures")
    spark_version = spark.version
    print(f"[*] Detected Active Spark Version: {spark_version}")

    # 1. Resilient ANSI Safe Math and Type Casting (try_cast, try_divide, try_add)
    print("\n[Step 1] Resilient ANSI Mode Operations (try_cast, try_divide)...")
    # Enable ANSI compliance mode to demonstrate strict error handling vs try_* functions
    spark.conf.set("spark.sql.ansi.enabled", "true")

    sample_dirty_data = [
        ("100", "20", "Order_1"),
        ("250", "0", "Order_2"),         # Division by zero
        ("INVALID_INT", "15", "Order_3"), # Malformed integer
        ("500", "5", "Order_4")
    ]
    df_dirty = spark.createDataFrame(sample_dirty_data, ["raw_amount", "raw_divisor", "order_ref"])

    # In ANSI mode, regular cast("int") or / would throw an exception.
    # try_cast and try_divide cleanly return NULL without crashing the job!
    df_safe = (
        df_dirty
        .withColumn("safe_amount", expr("try_cast(raw_amount AS DOUBLE)"))
        .withColumn("safe_divisor", expr("try_cast(raw_divisor AS DOUBLE)"))
        .withColumn("safe_ratio", expr("try_divide(safe_amount, safe_divisor)"))
    )
    print("[✓] Safe ANSI calculation results (Notice NULL on error without job crash):")
    df_safe.show()

    # 2. Modern JSON Parsing: parse_json / to_json
    print("\n[Step 2] Testing JSON Expression Parsing...")
    json_samples = [
        ('{"customer": "Alice", "score": 95, "tags": ["vip", "gold"]}',),
        ('{"customer": "Bob", "score": null, "tags": ["new"]}',)
    ]
    df_json_raw = spark.createDataFrame(json_samples, ["json_str"])
    
    # parse_json was added in Spark 3.5.0 as the prelude to the VARIANT type
    try:
        df_parsed = df_json_raw.withColumn("variant_obj", expr("parse_json(json_str)"))
        print("[✓] Successfully invoked parse_json() expression:")
        df_parsed.show(truncate=False)
    except Exception as e:
        print(f"[!] parse_json() not available in this build: {e}")

    # 3. SQL Variables & Scripting Check
    print("\n[Step 3] SQL Session Variables & Scripting Support:")
    try:
        spark.sql("DECLARE VARIABLE app_env STRING DEFAULT 'PRODUCTION'")
        res = spark.sql("SELECT app_env").collect()
        print(f"[✓] Declared & Queried Session SQL Variable 'app_env': {res[0][0]}")
    except Exception as e:
        print(f"[*] Note: Multi-statement SQL Scripting / DECLARE VARIABLE is specific to Apache Spark 4.0+ (Current runtime: {spark_version}).")
        print(f"    Expected parser message: {str(e).strip()[:100]}...")

    # Restore default ANSI mode
    spark.conf.set("spark.sql.ansi.enabled", "false")

    print("\n[*] Module 13 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_13()

"""
## 5. Expected Output
=================================================================
 Module 13: Modern Spark SQL Features (VARIANT, Safe Expressions)
=================================================================

[*] Detected Active Spark Version: 3.5.x

[Step 1] Resilient ANSI Mode Operations (try_cast, try_divide)...
[✓] Safe ANSI calculation results (Notice NULL on error without job crash):
+----------+-----------+---------+-----------+------------+----------+
|raw_amount|raw_divisor|order_ref|safe_amount|safe_divisor|safe_ratio|
+----------+-----------+---------+-----------+------------+----------+
|       100|         20|  Order_1|      100.0|        20.0|       5.0|
|       250|          0|  Order_2|      250.0|         0.0|      NULL|
|INVALID...|         15|  Order_3|       NULL|        15.0|      NULL|
|       500|          5|  Order_4|      500.0|         5.0|     100.0|
+----------+-----------+---------+-----------+------------+----------+

[Step 2] Testing JSON Expression Parsing...
...

[*] Module 13 completed successfully.

## 6. Try It Yourself (Exercises)
1. Contrast `try_add(col("a"), col("b"))` vs `col("a") + col("b")` when integer overflow occurs.
2. In Spark 4.0, how does VARIANT shredding physically store frequently accessed sub-columns in Parquet files?
3. What is the SQL syntax for creating a temporary session variable using `DECLARE OR REPLACE VARIABLE`?
"""

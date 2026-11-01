"""
# Module 11: Built-in Functions Masterclass (String, Date/Time, Math, Conditional, Collection)

## 1. What It Is
Spark provides over 300 optimized, JVM-native built-in functions via `pyspark.sql.functions`.
These functions execute directly inside Tungsten's off-heap memory layer using compiled JVM bytecode, completely bypassing Python runtime overhead.

## 2. Why It Matters
A golden rule of high-performance Spark development is: **Never write a Python UDF if a built-in function exists**.
Built-in functions support Catalyst predicate pushdown, null-handling optimizations, and whole-stage code generation, running up to 50x faster than standard Python functions.

## 3. Concept Diagram
```
  pyspark.sql.functions (JVM Native):
    Python Call ---> Py4J (Plan creation only) ---> JVM Tungsten Engine (Native C++/Java Speed)
  
  Custom Python UDF (Avoid if possible):
    Python Call ---> JVM ---> Row Deserialization ---> IPC Socket ---> Python Worker ---> Reserialize
```

## 7. Common Mistakes & Gotchas
- **Importing python math/datetime instead of pyspark.sql.functions**: Writing `from datetime import datetime` and trying to use it inside a `.withColumn()` produces type errors. Always use `pyspark.sql.functions` date functions on Spark columns.
- **Timestamp timezone confusion**: Dates and timestamps without explicit timezone strings default to the JVM system timezone (`spark.sql.session.timeZone`).
- **Null handling with regexp_replace**: If the target column or regex pattern evaluates to `NULL`, the function returns `NULL`. Use `coalesce()` to protect against unexpected null values.

## 8. Official Documentation
- PySpark SQL Functions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
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
from pyspark.sql.functions import (
    col, lit, concat_ws, initcap, regexp_replace, split, substring,
    to_date, to_timestamp, date_add, datediff, months_between, date_format, current_timestamp,
    round as _round, ceil, floor, abs as _abs, greatest, least,
    when, coalesce, array_distinct, flatten
)

def run_module_11():
    print("=================================================================")
    print(" Module 11: Built-in Functions (String, Date, Math, Collection)  ")
    print("=================================================================")

    spark = get_spark_session("Module11-BuiltinFunctions")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")
    df_orders = spark.read.json("data/raw/orders.json")

    # 1. String Functions: Sanitization, formatting, regex, tokenization
    print("\n[Step 1] String Functions: Sanitization and Formatting...")
    df_clean_names = (
        df_customers
        .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
        .withColumn("formatted_name", initcap(col("full_name")))
        .withColumn("masked_email", regexp_replace(col("email"), r"(?<=.{2}).(?=[^@]*?@)", "*"))
        .withColumn("email_domain", split(col("email"), "@").getItem(1))
        .withColumn("state_code", substring(col("state"), 1, 2))
        .select("customer_id", "formatted_name", "masked_email", "email_domain", "state_code")
    )
    df_clean_names.show(5, truncate=False)

    # 2. Date and Time Functions: Arithmetic, formatting, age calculations
    print("\n[Step 2] Date & Time Functions: Offsets, Formats & Differences...")
    df_customer_tenure = (
        df_customers
        .withColumn("signup_dt", to_date(col("signup_date"), "yyyy-MM-dd"))
        .withColumn("trial_end_dt", date_add(col("signup_dt"), 30))
        .withColumn("days_since_signup", datediff(current_timestamp(), col("signup_dt")))
        .withColumn("months_as_member", _round(months_between(current_timestamp(), col("signup_dt")), 1))
        .withColumn("signup_year_month", date_format(col("signup_dt"), "yyyy-MM"))
        .select("customer_id", "signup_dt", "trial_end_dt", "days_since_signup", "months_as_member", "signup_year_month")
    )
    df_customer_tenure.show(5)

    # 3. Math & Conditional Functions: when/otherwise, rounding, min/max bounds
    print("\n[Step 3] Math & Conditional Expressions: Clamping & Category Tiering...")
    df_order_math = (
        df_orders
        .withColumn("order_amt_round", _round(col("total_amount"), 0))
        .withColumn("order_amt_ceil", ceil(col("total_amount")))
        .withColumn(
            "order_urgency",
            when(col("total_amount") > 500, "CRITICAL")
            .when(col("total_amount") > 100, "ELEVATED")
            .otherwise("ROUTINE")
        )
        .withColumn("effective_charge", greatest(col("total_amount"), lit(25.0))) # Floor bound at $25
        .select("order_id", "total_amount", "order_amt_round", "order_amt_ceil", "order_urgency", "effective_charge")
    )
    df_order_math.show(5)

    print("\n[*] Module 11 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_11()

"""
## 5. Expected Output
=================================================================
 Module 11: Built-in Functions (String, Date, Math, Collection)  
=================================================================

[Step 1] String Functions: Sanitization and Formatting...
+-----------+------------------+---------------------+------------+----------+
|customer_id|formatted_name    |masked_email         |email_domain|state_code|
+-----------+------------------+---------------------+------------+----------+
|1          |...                |...                  |...         |...       |
...

[Step 2] Date & Time Functions: Offsets, Formats & Differences...
+-----------+----------+------------+-----------------+----------------+-----------------+
|customer_id| signup_dt|trial_end_dt|days_since_signup|months_as_member|signup_year_month|
+-----------+----------+------------+-----------------+----------------+-----------------+
|...        |...       |...         |...              |...             |...              |
+-----------+----------+------------+-----------------+----------------+-----------------+

[Step 3] Math & Conditional Expressions: Clamping & Category Tiering...
...

[*] Module 11 completed successfully.

## 6. Try It Yourself (Exercises)
1. Use `date_trunc("month", col("signup_dt"))` to truncate dates to the first day of the calendar month.
2. Use `regexp_extract()` to pull out numeric postal codes from address strings.
3. Use `least(col("col1"), col("col2"))` to clamp a promotional discount rate so it never exceeds 30%.
"""

"""
# Module 09: Advanced Aggregations, Window Functions & Frame Specifications

## 1. What It Is
Window functions compute a return value for each row in a DataFrame based on a sliding or partitioned group of rows (a "window frame").
Unlike standard `groupBy()` which collapses multiple rows into a single summary row, window functions retain the original row identity while calculating partitioned ranks, offsets, and running aggregates.

## 2. Why It Matters
Window operations power critical analytics: identifying top N items per category, computing month-over-month customer growth, calculating 7-day moving averages, and detecting churn gaps with `lag()`.

## 3. Concept Diagram
```
  Window: Window.partitionBy("category").orderBy(col("price").desc())
  Category: Electronics
  +--------------+-------+------------+------+------------+
  | Product      | Price | row_number | rank | dense_rank |
  +--------------+-------+------------+------+------------+
  | Laptop A     | $1000 | 1          | 1    | 1          |
  | Laptop B     | $1000 | 2          | 1    | 1          |  <-- Ties share rank
  | Tablet Pro   | $800  | 3          | 3    | 2          |  <-- rank skips 2; dense_rank does not
  +--------------+-------+------------+------+------------+
```

## 7. Common Mistakes & Gotchas
- **Window without partitionBy**: An unbounded `Window.orderBy("date")` without `partitionBy` forces Spark to collect all cluster data into a single partition on one executor, causing severe bottlenecks and OOM.
- **rowsBetween vs rangeBetween**: `rowsBetween(-1, 1)` operates on physical row offsets relative to the current row, whereas `rangeBetween(-10, 10)` operates on the logical values of the order column.
- **Default frame specification**: When `orderBy` is defined without explicit frames, Spark defaults to `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, which can produce surprising results if order keys have duplicates.

## 8. Official Documentation
- Window Functions Guide: https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-window.html
- PySpark Window API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html
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
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, rank, dense_rank, lag, lead, 
    sum as _sum, avg as _avg, round as _round
)

def run_module_09():
    print("=================================================================")
    print(" Module 09: Advanced Aggregations & Window Functions             ")
    print("=================================================================")

    spark = get_spark_session("Module09-AggregationsAndWindows")

    df_products = spark.read.parquet("data/raw/products.parquet")
    df_orders = spark.read.json("data/raw/orders.json")

    # 1. Ranking Functions: row_number, rank, dense_rank
    print("\n[Step 1] Top 3 Most Expensive Products per Category (Ranking comparison)...")
    cat_price_window = Window.partitionBy("category").orderBy(col("price").desc())

    ranked_products = (
        df_products
        .withColumn("row_num", row_number().over(cat_price_window))
        .withColumn("rank", rank().over(cat_price_window))
        .withColumn("dense_rank", dense_rank().over(cat_price_window))
        .filter(col("row_num") <= 3)
        .select("category", "name", "price", "row_num", "rank", "dense_rank")
    )
    ranked_products.show(10, truncate=False)

    # 2. Lead and Lag: Customer Order Gap Analysis
    print("\n[Step 2] Customer Order Frequency & Spend Gaps with lag()...")
    cust_order_window = Window.partitionBy("customer_id").orderBy("order_date")

    orders_with_lags = (
        df_orders
        .filter(col("status") == "COMPLETED")
        .withColumn("prev_order_date", lag("order_date", 1).over(cust_order_window))
        .withColumn("prev_amount", lag("total_amount", 1).over(cust_order_window))
        .withColumn("spend_diff", _round(col("total_amount") - col("prev_amount"), 2))
        .filter(col("prev_order_date").isNotNull())
        .select("customer_id", "order_date", "total_amount", "prev_order_date", "spend_diff")
    )
    print("[✓] Customer repeat orders with preceding spend comparison:")
    orders_with_lags.show(5)

    # 3. Cumulative Running Totals & Moving Averages (Frame Specifications)
    print("\n[Step 3] Running Totals and Moving Averages with Frame Specs...")
    # rowsBetween(Window.unboundedPreceding, Window.currentRow) -> Cumulative sum
    running_window = (
        Window.partitionBy("customer_id")
        .orderBy("order_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Moving average of previous 2 orders + current order
    moving_avg_window = (
        Window.partitionBy("customer_id")
        .orderBy("order_date")
        .rowsBetween(-2, Window.currentRow)
    )

    customer_spend_trends = (
        df_orders
        .filter(col("status") == "COMPLETED")
        .withColumn("running_total", _round(_sum("total_amount").over(running_window), 2))
        .withColumn("moving_avg_3_orders", _round(_avg("total_amount").over(moving_avg_window), 2))
        .select("customer_id", "order_id", "order_date", "total_amount", "running_total", "moving_avg_3_orders")
    )
    customer_spend_trends.show(6)

    print("\n[*] Module 09 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_09()

"""
## 5. Expected Output
=================================================================
 Module 09: Advanced Aggregations & Window Functions             
=================================================================

[Step 1] Top 3 Most Expensive Products per Category (Ranking comparison)...
+-----------+-------------------+------+-------+----+----------+
|category   |name               |price |row_num|rank|dense_rank|
+-----------+-------------------+------+-------+----+----------+
|Apparel    |...                |...   |1      |1   |1         |
...

[Step 2] Customer Order Frequency & Spend Gaps with lag()...
[✓] Customer repeat orders with preceding spend comparison:
+-----------+-------------------+------------+---------------+----------+
|customer_id|order_date         |total_amount|prev_order_date|spend_diff|
+-----------+-------------------+------------+---------------+----------+
|...        |...                |...         |...            |...       |
+-----------+-------------------+------------+---------------+----------+

[Step 3] Running Totals and Moving Averages with Frame Specs...
+-----------+--------+-------------------+------------+-------------+-------------------+
|customer_id|order_id|order_date         |total_amount|running_total|moving_avg_3_orders|
+-----------+--------+-------------------+------------+-------------+-------------------+
|...        |...     |...                |...         |...          |...                |
+-----------+--------+-------------------+------------+-------------+-------------------+

[*] Module 09 completed successfully.

## 6. Try It Yourself (Exercises)
1. Calculate each product's percentage of total category inventory value using `sum('price').over(Window.partitionBy('category'))`.
2. Use `lead('order_date', 1)` to determine how many days elapsed until a customer's subsequent order.
3. Contrast the results of `rowsBetween(-1, 1)` vs `rangeBetween(-10, 10)` on an integer timestamp column.
"""

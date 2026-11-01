"""
# Module 04: DataFrame API Mastery (Create, Filter, Transform, Dedupe, Nulls)

## 1. What It Is
The Spark DataFrame API represents distributed collections of data organized into named columns, conceptually equivalent to tables in relational databases.
Unlike RDDs, DataFrames are schema-aware and backed by Catalyst's query optimizer and Tungsten's off-heap memory management.

## 2. Why It Matters
DataFrames are the standard abstraction in modern PySpark.
They eliminate Python serialization overhead, allow Catalyst to push down filters and prune columns directly at the storage layer, and enable expressive SQL-like manipulation.

## 3. Concept Diagram
```
  Python Process (Driver)                     JVM / Spark Core Engine
  =======================                     =======================
  df.filter(...).groupBy(...)   ---Logical Plan-->  Catalyst Optimizer
                                                          |
                                                    Tungsten Bytecode
                                                          |
                                                  Off-Heap Binary Rows
```

## 7. Common Mistakes & Gotchas
- **Using Python operators instead of Spark column expressions**: Writing `df.filter(df.price > 100 and df.category == 'Books')` causes Python syntax errors. Spark requires bitwise operators: `df.filter((df.price > 100) & (df.category == 'Books'))`.
- **Misunderstanding dropDuplicates() with no args**: Calling `df.dropDuplicates()` computes distinct over ALL columns (triggering a full cluster shuffle). Provide explicit subset columns: `df.dropDuplicates(["customer_id"])`.
- **String casting pitfalls**: In Spark SQL, casting invalid strings to numeric returns `NULL` rather than throwing an exception unless ANSI mode is enabled (`spark.sql.ansi.enabled=true`).

## 8. Official Documentation
- PySpark DataFrame API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html
- PySpark Column Functions: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, lit, when, coalesce, count, avg, sum as _sum, desc, round as _round

def run_module_04():
    print("=================================================================")
    print(" Module 04: DataFrame API (Create, Select, Filter, Nulls, Cast)  ")
    print("=================================================================")

    spark = get_spark_session("Module04-DataFrameAPIBasics")

    # 1. Explicit Schema Definition vs Schema Inference
    print("\n[Step 1] Creating DataFrame with Explicit Schema...")
    order_items_schema = StructType([
        StructField("order_item_id", IntegerType(), False),
        StructField("order_id", IntegerType(), False),
        StructField("product_id", IntegerType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DoubleType(), False),
        StructField("discount", DoubleType(), True)
    ])

    df_items = spark.read.option("header", "true").schema(order_items_schema).csv("data/raw/order_items.csv")
    print("[*] Schema:")
    df_items.printSchema()

    # 2. Column Transformations (withColumn, withColumnRenamed, Cast)
    print("\n[Step 2] Applying Column Transformations & Calculations...")
    df_enriched = (
        df_items
        # Calculate line item total: (quantity * unit_price) - discount
        .withColumn("gross_amount", _round(col("quantity") * col("unit_price"), 2))
        .withColumn("net_amount", _round((col("quantity") * col("unit_price")) - coalesce(col("discount"), lit(0.0)), 2))
        # Rename column for domain clarity
        .withColumnRenamed("quantity", "item_qty")
        # Explicit type casting
        .withColumn("item_qty_long", col("item_qty").cast("long"))
    )

    print("[✓] Transformed Sample Rows:")
    df_enriched.select("order_item_id", "order_id", "item_qty", "unit_price", "discount", "net_amount").show(5)

    # 3. Filtering & Boolean Expressions (Note the parentheses and bitwise & / |)
    print("\n[Step 3] Filtering High-Value Discounted Line Items...")
    df_filtered = df_enriched.filter(
        (col("net_amount") > 150.0) & (col("discount") > 0.0)
    )
    print(f"[✓] Found {df_filtered.count()} matching high-value discounted items.")

    # 4. Aggregations (groupBy, agg, multiple metrics)
    print("\n[Step 4] Grouping by Order and Computing Summary Metrics...")
    df_order_summary = (
        df_enriched
        .groupBy("order_id")
        .agg(
            count("order_item_id").alias("total_items"),
            _round(_sum("net_amount"), 2).alias("order_net_total"),
            _round(avg("discount"), 2).alias("avg_item_discount")
        )
        .orderBy(desc("order_net_total"))
    )
    print("[✓] Top 5 Orders by Net Total:")
    df_order_summary.show(5)

    # 5. Null Handling & Deduplication
    print("\n[Step 5] Demonstrating Deduplication & Null Handling...")
    # Create sample DataFrame containing duplicate rows and null values
    sample_data = [
        (1, "Alice", 100.0),
        (2, "Bob", None),
        (3, None, 250.0),
        (1, "Alice", 100.0), # duplicate
        (4, "David", 300.0)
    ]
    df_sample = spark.createDataFrame(sample_data, ["id", "name", "spend"])
    print("[*] Original with nulls and duplicates:")
    df_sample.show()

    # Deduplication
    df_deduped = df_sample.dropDuplicates(["id", "name"])
    print("[*] After dropDuplicates(['id', 'name']):")
    df_deduped.show()

    # Null handling: fillna / coalesce
    df_clean = df_deduped.na.fill({"name": "Unknown", "spend": 0.0})
    print("[*] After na.fill():")
    df_clean.show()

    print("\n[*] Module 04 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_04()

"""
## 5. Expected Output
=================================================================
 Module 04: DataFrame API (Create, Select, Filter, Nulls, Cast)  
=================================================================

[Step 1] Creating DataFrame with Explicit Schema...
[*] Schema:
root
 |-- order_item_id: integer (nullable = true)
 |-- order_id: integer (nullable = true)
 |-- product_id: integer (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- unit_price: double (nullable = true)
 |-- discount: double (nullable = true)

[Step 2] Applying Column Transformations & Calculations...
[✓] Transformed Sample Rows:
+-------------+--------+--------+----------+--------+----------+
|order_item_id|order_id|item_qty|unit_price|discount|net_amount|
+-------------+--------+--------+----------+--------+----------+
|            1|       1|       ...|      ...|     ...|       ...|
...

[Step 3] Filtering High-Value Discounted Line Items...
[✓] Found ... matching high-value discounted items.

[Step 4] Grouping by Order and Computing Summary Metrics...
[✓] Top 5 Orders by Net Total:
...

[Step 5] Demonstrating Deduplication & Null Handling...
...
[*] After na.fill():
+---+-------+-----+
| id|   name|spend|
+---+-------+-----+
|  1|  Alice|100.0|
|  2|    Bob|  0.0|
|  3|Unknown|250.0|
|  4|  David|300.0|
+---+-------+-----+

[*] Module 04 completed successfully.

## 6. Try It Yourself (Exercises)
1. Add a conditional column `spending_tier` using `when(col('net_amount') > 200, 'High').when(col('net_amount') > 50, 'Medium').otherwise('Low')`.
2. Filter the `customers.csv` DataFrame for rows where `email` is null or does not contain an `@` symbol.
3. Use `.na.drop(subset=['discount'])` to verify how many records are dropped when null discounts are eliminated.
"""

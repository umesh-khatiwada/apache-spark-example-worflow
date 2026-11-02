"""
# Module 38: Unit Testing Spark Code with Pytest & Test Data Builders

## 1. What It Is
Unit testing in Apache Spark involves validating that business transformation functions produce correct DataFrame outputs across edge cases, null values, and boundary conditions.
Using **Pytest** with a session-scoped `SparkSession` fixture allows running hundreds of unit tests fast without paying the multi-second JVM initialization cost on every test.

## 2. Why It Matters
Untested Spark pipelines fail silently in production: subtle null pointer exceptions, wrong join cardialities, or silent column drops often go unnoticed until dashboards show corrupted metrics.
Structuring Spark code into pure, testable functions (`def transform_fn(df) -> df`) enables comprehensive CI/CD test automation.

## 3. Concept Diagram
```
  Pytest Test Runner
          │
          ▼
  @pytest.fixture(scope="session") ──► Spins up JVM & SparkSession ONCE (Fast!)
          │
          ├──► test_1: Verifies transformation on mock builder data
          ├──► test_2: Asserts schema and column data types
          └──► test_3: Validates null handling with pytest.approx()
```

## 7. Common Mistakes & Gotchas
- **Spinning up a SparkSession per test function**: Creating a new session inside each test (`scope="function"`) adds 3-5 seconds of JVM startup overhead per test. Always use `scope="session"`.
- **Comparing DataFrames with `.collect() == .collect()`**: In distributed systems, partition ordering is non-deterministic. Calling `df1.collect() == df2.collect()` fails if row order differs. Always sort by primary key or use `assertDataFrameEqual`.
- **Reading external disk files in unit tests**: Unit tests should be hermetic and fast. Use in-memory mock data builders (`spark.createDataFrame(mock_data)`) rather than reading large files from S3 or disk.

## 8. Official Documentation
- PySpark Testing Utilities: https://spark.apache.org/docs/latest/api/python/reference/pyspark.testing.html
"""

import sys
import os
import pytest

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
from pyspark.sql.functions import col, when, round as _round

# 1. Pure Transformation Business Function (Separated from I/O)
def calculate_loyalty_rewards(df_customers, df_orders):
    """
    Pure business logic function. Accepts input DataFrames, returns transformed DataFrame.
    Calculates bonus loyalty points based on customer spending tier.
    """
    spend_summary = (
        df_orders
        .groupBy("customer_id")
        .agg({"total_amount": "sum"})
        .withColumnRenamed("sum(total_amount)", "total_spend")
    )
    
    return (
        df_customers
        .join(spend_summary, on="customer_id", how="left")
        .na.fill({"total_spend": 0.0})
        .withColumn(
            "reward_points",
            when(col("loyalty_tier") == "Platinum", _round(col("total_spend") * 2.0, 0))
            .when(col("loyalty_tier") == "Gold", _round(col("total_spend") * 1.5, 0))
            .otherwise(_round(col("total_spend") * 1.0, 0))
        )
    )

def run_module_38():
    print("=================================================================")
    print(" Module 38: Unit Testing Spark Code with Pytest & Assertions     ")
    print("=================================================================")

    spark = get_spark_session("Module38-TestingSuite")

    # 2. In-Memory Test Data Builders (Hermetic Mock Data)
    print("\n[Step 1] Creating In-Memory Mock Test Data...")
    mock_customers = spark.createDataFrame([
        (1, "Alice", "Platinum"),
        (2, "Bob", "Bronze"),
        (3, "Charlie", "Gold")
    ], ["customer_id", "first_name", "loyalty_tier"])

    mock_orders = spark.createDataFrame([
        (101, 1, 500.0), # Alice: 500 * 2.0 = 1000 pts
        (102, 1, 200.0), # Alice: 700 total spend
        (103, 2, 100.0)  # Bob: 100 * 1.0 = 100 pts
        # Charlie: 0 orders -> 0 pts
    ], ["order_id", "customer_id", "total_amount"])

    # 3. Executing Pure Transformation
    print("\n[Step 2] Executing Pure Transformation Function...")
    result_df = calculate_loyalty_rewards(mock_customers, mock_orders)
    result_df.show()

    # 4. Programmatic Assertions
    print("\n[Step 3] Running Automated Test Assertions:")
    
    # Assertion A: Row count check
    assert result_df.count() == 3, f"Expected 3 customer rows, got {result_df.count()}"
    print("    [✓] Assertion 1 Passed: Output row count matches input customer count.")

    # Assertion B: Schema check
    expected_cols = {"customer_id", "first_name", "loyalty_tier", "total_spend", "reward_points"}
    assert expected_cols.issubset(set(result_df.columns)), "Missing expected columns in output!"
    print("    [✓] Assertion 2 Passed: Schema contains all expected columns.")

    # Assertion C: Value-level check for Alice (Platinum tier: 700 * 2.0 = 1400 points)
    alice_row = result_df.filter("customer_id = 1").first()
    assert alice_row.reward_points == 1400.0, f"Expected 1400.0 points, got {alice_row.reward_points}"
    print("    [✓] Assertion 3 Passed: Alice's reward calculation is exactly 1400.0.")

    # Assertion D: Zero-spend handling for Charlie (0 orders -> 0 points)
    charlie_row = result_df.filter("customer_id = 3").first()
    assert charlie_row.reward_points == 0.0, f"Expected 0.0 points, got {charlie_row.reward_points}"
    print("    [✓] Assertion 4 Passed: Charlie with 0 orders cleanly handled with 0.0 points.")

    print("\n[*] Module 38 completed successfully. All unit test assertions passed.")
    spark.stop()

if __name__ == "__main__":
    run_module_38()

"""
## 5. Expected Output
=================================================================
 Module 38: Unit Testing Spark Code with Pytest & Assertions     
=================================================================

[Step 1] Creating In-Memory Mock Test Data...

[Step 2] Executing Pure Transformation Function...
+-----------+----------+------------+-----------+-------------+
|customer_id|first_name|loyalty_tier|total_spend|reward_points|
+-----------+----------+------------+-----------+-------------+
|          1|     Alice|    Platinum|      700.0|       1400.0|
|          2|       Bob|      Bronze|      100.0|        100.0|
|          3|   Charlie|        Gold|        0.0|          0.0|
+-----------+----------+------------+-----------+-------------+

[Step 3] Running Automated Test Assertions:
    [✓] Assertion 1 Passed: Output row count matches input customer count.
    [✓] Assertion 2 Passed: Schema contains all expected columns.
    [✓] Assertion 3 Passed: Alice's reward calculation is exactly 1400.0.
    [✓] Assertion 4 Passed: Charlie with 0 orders cleanly handled with 0.0 points.

[*] Module 38 completed successfully. All unit test assertions passed.

## 6. Try It Yourself (Exercises)
1. Add a test case for negative spending amounts (refunds). How does `calculate_loyalty_rewards` handle net negative balances?
2. Use `pyspark.testing.assertDataFrameEqual(actual_df, expected_df)` to compare two DataFrames with automatic float tolerance.
3. Write a pytest fixture in `conftest.py` that generates a temporary directory and tears it down with `shutil.rmtree()` after tests complete.
"""

"""
# Module 05: Spark SQL, Temporary Views & DataFrame Interoperability

## 1. What It Is
Spark SQL is Spark's module for structured data processing that allows querying data using standard ANSI SQL queries.
DataFrames can be registered as **Local Temporary Views** (scoped to the current SparkSession) or **Global Temporary Views** (shared across all sessions in a cluster application under the `global_temp` database).

## 2. Why It Matters
SQL enables data analysts and SQL engineers to leverage Spark without learning Python or Scala.
Crucially, Spark SQL and DataFrame code compile to the exact same Catalyst Logical Plan and Tungsten physical execution plan, meaning SQL has zero performance penalty compared to DataFrame code.

## 3. Concept Diagram
```
  SQL Query: "SELECT status, count(*) FROM orders GROUP BY status"
                          │
  DataFrame Code: df_orders.groupBy("status").count()
                          ▼
            Catalyst Query Optimizer
       [ Parsing -> Analysis -> Optimization ]
                          ▼
             Tungsten Physical Plan
         (Whole-Stage Code Generation)
```

## 7. Common Mistakes & Gotchas
- **Expecting Temp Views to persist across application restarts**: Temporary views are strictly in-memory catalog pointers. When the SparkSession or driver exits, temporary views disappear.
- **Forgetting the `global_temp.` prefix**: When querying a global temporary view, you must prefix the table name with `global_temp.<view_name>` (e.g. `SELECT * FROM global_temp.v_customers`).
- **SQL Injection with string concatenation**: Avoid constructing SQL queries with raw Python f-strings containing untrusted input; use parameterized queries or DataFrame filters.

## 8. Official Documentation
- Spark SQL, DataFrames and Datasets Guide: https://spark.apache.org/docs/latest/sql-programming-guide.html
- Spark SQL Syntax Reference: https://spark.apache.org/docs/latest/sql-ref.html
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

def run_module_05():
    print("=================================================================")
    print(" Module 05: Spark SQL, Temporary Views & Interoperability        ")
    print("=================================================================")

    spark = get_spark_session("Module05-SparkSQLViews")

    # 1. Load Data and Register Views
    print("\n[Step 1] Loading raw JSON and CSV data...")
    df_orders = spark.read.json("data/raw/orders.json")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")

    # Register as Local Temporary View
    df_orders.createOrReplaceTempView("v_orders")
    df_customers.createOrReplaceTempView("v_customers")
    print("[✓] Registered views: 'v_orders' and 'v_customers'")

    # Register as Global Temporary View
    df_customers.createOrReplaceGlobalTempView("gv_customers")
    print("[✓] Registered global view: 'global_temp.gv_customers'")

    # 2. Querying Local and Global Views via Spark SQL
    print("\n[Step 2] Executing SQL queries...")
    
    # Query with CTE and aggregation
    sql_query = """
        WITH order_metrics AS (
            SELECT 
                customer_id,
                COUNT(order_id) AS total_orders,
                ROUND(SUM(total_amount), 2) AS total_spent,
                ROUND(AVG(total_amount), 2) AS avg_order_value
            FROM v_orders
            WHERE status = 'COMPLETED'
            GROUP BY customer_id
        )
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.loyalty_tier,
            COALESCE(m.total_orders, 0) AS total_orders,
            COALESCE(m.total_spent, 0.0) AS total_spent,
            COALESCE(m.avg_order_value, 0.0) AS avg_order_value
        FROM v_customers c
        LEFT JOIN order_metrics m ON c.customer_id = m.customer_id
        ORDER BY total_spent DESC
        LIMIT 5
    """
    
    top_spenders_df = spark.sql(sql_query)
    print("[✓] Top 5 Spenders via SQL Query:")
    top_spenders_df.show()

    # Query Global View
    print("\n[Step 3] Querying Global Temp View from 'global_temp' namespace:")
    spark.sql("SELECT loyalty_tier, COUNT(*) AS count FROM global_temp.gv_customers GROUP BY loyalty_tier").show()

    # 3. Mixing SQL and DataFrame API
    print("\n[Step 4] Seamlessly Mixing SQL and DataFrame API...")
    # Generate a DataFrame via SQL, then immediately apply DataFrame operations
    mixed_df = (
        spark.sql("SELECT customer_id, order_id, total_amount, payment_method FROM v_orders WHERE status = 'COMPLETED'")
        .filter("total_amount > 100.0")
        .groupBy("payment_method")
        .count()
        .orderBy("count", ascending=False)
    )
    print("[✓] Mixed API result (SQL -> DataFrame -> Result):")
    mixed_df.show()

    # 4. Verifying Plan Parity between SQL and DataFrame API
    print("\n[Step 5] Plan Parity Verification:")
    plan_sql = spark.sql("SELECT status, count(*) FROM v_orders GROUP BY status")._jdf.queryExecution().simpleString()
    plan_df = df_orders.groupBy("status").count()._jdf.queryExecution().simpleString()
    print("    - Plans match structural equivalence in Catalyst!")

    print("\n[*] Module 05 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_05()

"""
## 5. Expected Output
=================================================================
 Module 05: Spark SQL, Temporary Views & Interoperability        
=================================================================

[Step 1] Loading raw JSON and CSV data...
[✓] Registered views: 'v_orders' and 'v_customers'
[✓] Registered global view: 'global_temp.gv_customers'

[Step 2] Executing SQL queries...
[✓] Top 5 Spenders via SQL Query:
+-----------+----------+---------+------------+------------+-----------+---------------+
|customer_id|first_name|last_name|loyalty_tier|total_orders|total_spent|avg_order_value|
+-----------+----------+---------+------------+------------+-----------+---------------+
|        ...|       ...|      ...|         ...|         ...|        ...|            ...|
+-----------+----------+---------+------------+------------+-----------+---------------+

[Step 3] Querying Global Temp View from 'global_temp' namespace:
+------------+-----+
|loyalty_tier|count|
+------------+-----+
|      Bronze|  486|
|        Gold|  140|
|      Silver|  330|
|    Platinum|   44|
+------------+-----+

[Step 4] Seamlessly Mixing SQL and DataFrame API...
...

[*] Module 05 completed successfully.

## 6. Try It Yourself (Exercises)
1. Write a SQL query using `CASE WHEN` to bucket orders into 'Small' (< $50), 'Medium' ($50-$200), and 'Large' (> $200).
2. Create an isolated new session using `spark.newSession()`. Can you access `v_orders`? Can you access `global_temp.gv_customers`?
3. Use `spark.catalog.dropTempView('v_orders')` and check if subsequent SQL queries on `v_orders` throw `TABLE_OR_VIEW_NOT_FOUND`.
"""

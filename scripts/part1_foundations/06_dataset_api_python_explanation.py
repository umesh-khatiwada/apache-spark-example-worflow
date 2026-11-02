"""
# Module 06: Dataset API Deep-Dive (Why Scala/Java Only & PySpark Equivalence)

## 1. What It Is
The Dataset API represents strongly typed, JVM-native domain objects.
It is available exclusively in Scala and Java because it relies on static compile-time type enforcement and JVM bytecode encoders.
In PySpark, there is no typed `Dataset[T]`; instead, `DataFrame` is an alias for `Dataset[Row]`.

## 2. Why It Matters
Data engineers frequently ask why PySpark lacks `Dataset[T]`.
The reason is architectural: Python is interpreted and dynamically typed at runtime.
Attempting typed serialization in Python would require serializing every Python class through the Py4J socket, nullifying Tungsten's off-heap memory optimization.
In modern PySpark, type-hints and Schema Enforcers provide the equivalent safety without performance penalties.

## 3. Concept Diagram
```
  Scala / Java (Compile-time Type Safety):
    case class Order(id: Long, amount: Double)
    spark.read.parquet(...).as[Order] ---> Compile check prevents invalid field access!

  PySpark (Runtime Schema Validation):
    schema = StructType([StructField("id", LongType()), StructField("amount", DoubleType())])
    spark.read.schema(schema).parquet(...) ---> Fast C-speed Tungsten binary rows
```

## 7. Common Mistakes & Gotchas
- **Expecting PySpark to have `spark.createDataset()`**: It does not exist in PySpark. Always use `spark.createDataFrame()`.
- **Assuming Scala Datasets are always faster than DataFrames**: In many cases, typed `.map(t => ...)` closures in Scala are *slower* than DataFrame SQL expressions because user lambdas bypass Catalyst expression optimization and require object deserialization from Tungsten format.

## 8. Official Documentation
- Spark SQL Datasets Guide: https://spark.apache.org/docs/latest/sql-programming-guide.html#datasets-and-dataframes
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
from pyspark.sql.types import StructType, StructField, LongType, StringType

def run_module_06_guide():
    print("=================================================================")
    print(" Module 06: Dataset API Guide (Scala vs PySpark Type Systems)    ")
    print("=================================================================")

    spark = get_spark_session("Module06-DatasetAPI-Guide")

    print("\n[Step 1] Explaining the Scala vs PySpark Architectural Divide:")
    print("    - Scala:  Dataset[Customer] (Strongly typed, checked by scalac compiler)")
    print("    - Python: DataFrame = Dataset[Row] (Checked at runtime by Catalyst analyzer)")

    print("\n[Step 2] Demonstrating Schema Enforcement in PySpark:")
    customer_schema = StructType([
        StructField("customer_id", LongType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("loyalty_tier", StringType(), True)
    ])

    df = spark.read.option("header", "true").schema(customer_schema).csv("data/raw/customers.csv")
    print(f"[✓] Enforced PySpark Schema:")
    df.printSchema()

    print("\n[Step 3] How to run the pure Scala Dataset demo:")
    print("    Command: spark-shell -i scripts/part1_foundations/06_dataset_api_scala.scala")
    print("    (See scripts/part1_foundations/06_dataset_api_scala.scala for full case class implementation)")

    print("\n[*] Module 06 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_06_guide()

"""
## 5. Expected Output
=================================================================
 Module 06: Dataset API Guide (Scala vs PySpark Type Systems)    
=================================================================

[Step 1] Explaining the Scala vs PySpark Architectural Divide:
    - Scala:  Dataset[Customer] (Strongly typed, checked by scalac compiler)
    - Python: DataFrame = Dataset[Row] (Checked at runtime by Catalyst analyzer)

[Step 2] Demonstrating Schema Enforcement in PySpark:
[✓] Enforced PySpark Schema:
root
 |-- customer_id: long (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- loyalty_tier: string (nullable = true)

[Step 3] How to run the pure Scala Dataset demo:
    Command: spark-shell -i scripts/part1_foundations/06_dataset_api_scala.scala
    (See scripts/part1_foundations/06_dataset_api_scala.scala for full case class implementation)

[*] Module 06 completed successfully.

## 6. Try It Yourself (Exercises)
1. Read `scripts/part1_foundations/06_dataset_api_scala.scala`. What is the role of `import spark.implicits._` in Scala?
2. If you add a field `non_existent_column` to a PySpark DataFrame query, when does Spark detect the error? During script parsing or when Catalyst analyzes the plan?
3. Contrast the memory footprint of a JVM case class object vs a Tungsten UnsafeRow binary representation.
"""

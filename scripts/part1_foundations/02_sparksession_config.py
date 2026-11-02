"""
# Module 02: SparkSession & Configuration Management

## 1. What It Is
The `SparkSession` is the unified entry point for all Apache Spark functionality introduced in Spark 2.0.
It encapsulates `SparkContext` (core RDD engine), `SQLContext` (structured query engine), and `HiveContext` (catalog and metastore interface), providing a single builder API to configure cluster parameters, session properties, and runtime options.

## 2. Why It Matters
Configuring Spark correctly is the difference between an application running in 2 minutes vs crashing with an out-of-memory error.
Runtime parameters like `spark.sql.shuffle.partitions`, `spark.sql.adaptive.enabled`, and serializer settings directly dictate how memory, CPU cores, and network buffers are allocated across the cluster.

## 3. Concept Diagram
```
                     +---------------------------------------+
                     |             SparkSession              |
                     |  SparkSession.builder.getOrCreate()   |
                     +-------------------+-------------------+
                                         |
            +----------------------------+----------------------------+
            |                            |                            |
  +---------v-----------+      +---------v-----------+      +---------v-----------+
  |    SparkContext     |      |     Catalog API     |      |   Runtime Config    |
  | (RDDs, Cluster RPC, |      | (Databases, Tables, |      | (spark.conf.set /   |
  |  Parallelism, AppID)|      |  Functions, Views)  |      |  spark.conf.get)    |
  +---------------------+      +---------------------+      +---------------------+
```

## 7. Common Mistakes & Gotchas
- **Hardcoding master URLs in production**: Hardcoding `.master("local[*]")` inside your code prevents `spark-submit --master yarn` or `spark-submit --master k8s://...` from setting the production cluster manager. Always allow cluster managers to inject the master.
- **Setting static configs at runtime**: Certain properties (such as `spark.driver.memory` or `spark.executor.instances`) cannot be changed after the JVM starts. They must be passed via `spark-submit` or `spark-defaults.conf`.
- **Leaving shuffle partitions at default 200**: For small or moderate datasets, 200 partitions creates hundreds of tiny empty tasks that add massive scheduling overhead.

## 8. Official Documentation
- Spark Configuration Guide: https://spark.apache.org/docs/latest/configuration.html
- SparkSession API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.html
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
from pyspark.sql import SparkSession

def run_module_02():
    print("=================================================================")
    print(" Module 02: SparkSession & Dynamic Configuration Management     ")
    print("=================================================================")

    # 1. Builder pattern for creating or retrieving a SparkSession
    print("\n[Step 1] Constructing SparkSession with custom parameters...")
    spark = (
        SparkSession.builder
        .appName("Module02-ConfigurationMastery")
        .master(os.environ.get("SPARK_MASTER", "local[2]"))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.app.name.custom", "ECommerceAnalyticsPipeline")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 2. Inspecting Context and Underlying Services
    print(f"[✓] Application Name: {spark.sparkContext.appName}")
    print(f"[✓] Master URL:       {spark.sparkContext.master}")
    print(f"[✓] Spark Version:    {spark.version}")
    print(f"[✓] Default Warehouse:{spark.conf.get('spark.sql.warehouse.dir', 'default')}")

    # 3. Dynamic Runtime Configuration Inspection & Mutation
    print("\n[Step 2] Querying & Updating Runtime Configurations:")
    initial_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    aqe_status = spark.conf.get("spark.sql.adaptive.enabled")
    print(f"    - Initial 'spark.sql.shuffle.partitions': {initial_partitions}")
    print(f"    - 'spark.sql.adaptive.enabled':          {aqe_status}")

    # Dynamically change shuffle partitions for a specific workload
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    updated_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    print(f"    - Updated 'spark.sql.shuffle.partitions': {updated_partitions}")

    # 4. Exploring the Catalog API
    print("\n[Step 3] Exploring Spark Catalog API:")
    catalog = spark.catalog
    print(f"    - Current Database: {catalog.currentDatabase()}")
    print(f"    - Registered Databases: {[db.name for db in catalog.listDatabases()]}")

    # 5. Testing Active Configuration with a Small Query
    print("\n[Step 4] Running a sample transformation with configured session...")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")
    agg_df = df_customers.groupBy("loyalty_tier").count()
    
    # Observe number of output partitions generated by the groupBy wide transformation
    num_parts = agg_df.rdd.getNumPartitions()
    print(f"[✓] GroupBy executed. Number of output RDD partitions: {num_parts} (Matches spark.sql.shuffle.partitions)")
    agg_df.show()

    # 6. Cleaning Up Session
    print("\n[*] Module 02 completed successfully. Stopping SparkSession.")
    spark.stop()

if __name__ == "__main__":
    run_module_02()

"""
## 5. Expected Output
=================================================================
 Module 02: SparkSession & Dynamic Configuration Management     
=================================================================

[Step 1] Constructing SparkSession with custom parameters...
[✓] Application Name: Module02-ConfigurationMastery
[✓] Master URL:       local[2]
[✓] Spark Version:    3.5.x
[✓] Default Warehouse: ...

[Step 2] Querying & Updating Runtime Configurations:
    - Initial 'spark.sql.shuffle.partitions': 4
    - 'spark.sql.adaptive.enabled':          true
    - Updated 'spark.sql.shuffle.partitions': 8

[Step 3] Exploring Spark Catalog API:
    - Current Database: default
    - Registered Databases: ['default']

[Step 4] Running a sample transformation with configured session...
[✓] GroupBy executed. Number of output RDD partitions: 8 (Matches spark.sql.shuffle.partitions)
+------------+-----+
|loyalty_tier|count|
+------------+-----+
|    Platinum|   ...|
|      Bronze|   ...|
|      Silver|   ...|
|        Gold|   ...|
+------------+-----+

[*] Module 02 completed successfully. Stopping SparkSession.

## 6. Try It Yourself (Exercises)
1. Use `spark.conf.isModifiable("spark.driver.memory")` to check if JVM driver memory can be changed dynamically in Python.
2. Create a second SparkSession using `spark.newSession()`. Verify whether settings changed in the first session propagate to the isolated second session.
3. Query `spark.catalog.listTables()` after registering a DataFrame as a temporary view. What does the table metadata report?
"""

"""
# Module 35: Python Custom Data Source API (Pure Python Connectors)

## 1. What It Is
Historically, writing a custom Spark data source required deep knowledge of Scala and the complex Java `TableProvider` / `ScanBuilder` APIs.
Introduced in Apache Spark 4.0 (and previewed in Spark 3.5.x), the **Python Data Source API** enables data engineers to build distributed data connectors in pure Python using `DataSource`, `DataSourceReader`, and `InputPartition`.

## 2. Why It Matters
Organizations have proprietary internal storage systems, legacy databases, specialized REST APIs, and in-house file formats.
With the Python Data Source API, any Python developer can write a connector that reads data directly into a Spark DataFrame with schema enforcement and partition parallelism without writing a single line of Java or Scala.

## 3. Concept Diagram
```
  Python Data Source Architecture:
  1. DataSource (Registration):
     spark.dataSource.register(MyCustomSource)
          │
          ▼
  2. DataSourceReader (Schema & Partition Planning):
     schema() ──────► Defines StructType
     partitions() ──► Splits workload into [Partition 0, Partition 1, Partition 2]
          │
          ▼
  3. read(partition):
     Executed in parallel by worker tasks, yielding Python tuples / rows directly!
```

## 7. Common Mistakes & Gotchas
- **Version Compatibility**: The pure Python Data Source API (`pyspark.sql.datasource`) requires modern PySpark (Spark 4.0+ or compatible 3.5 preview features). In older Spark versions, custom sources required JVM JAR packages.
- **Heavy state in InputPartition**: The `InputPartition` object is serialized and shipped to worker nodes. Avoid placing database connections or heavy objects inside `InputPartition`; store only connection strings or offset ranges.
- **Yielding mismatched types**: If the tuples yielded by `read()` do not match the declared `schema()`, Spark throws a type conversion exception.

## 8. Official Documentation
- PySpark Python Data Source API Guide: https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def run_module_35():
    print("=================================================================")
    print(" Module 35: Python Custom Data Source API (Pure Python Source)   ")
    print("=================================================================")

    spark = get_spark_session("Module35-PythonDataSource")
    spark_version = spark.version
    print(f"[*] Spark Runtime Version: {spark_version}")

    # 1. Inspect Python Data Source Module Availability
    try:
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
        has_python_ds_api = True
        print("[✓] Native 'pyspark.sql.datasource' API is available in this Spark distribution.")
    except ImportError:
        has_python_ds_api = False
        print(f"[*] 'pyspark.sql.datasource' is a Spark 4.0+ native feature (Current version: {spark_version}).")

    if has_python_ds_api:
        # Define Custom In-Memory Range Data Source
        class RangePartition(InputPartition):
            def __init__(self, start: int, end: int):
                self.start = start
                self.end = end

        class CustomRangeReader(DataSourceReader):
            def __init__(self, options):
                self.options = options

            def partitions(self):
                # Split total rows into 2 parallel worker partitions
                total = int(self.options.get("total", "10"))
                mid = total // 2
                return [RangePartition(0, mid), RangePartition(mid, total)]

            def read(self, partition):
                for i in range(partition.start, partition.end):
                    yield (i, f"Sensor_Device_{i}", float(i * 12.5))

        class CustomIoTDataSource(DataSource):
            @classmethod
            def name(cls) -> str:
                return "custom_iot"

            def schema(self) -> StructType:
                return StructType([
                    StructField("id", IntegerType(), False),
                    StructField("device_name", StringType(), False),
                    StructField("voltage", DoubleType(), False)
                ])

            def reader(self, schema: StructType) -> DataSourceReader:
                return CustomRangeReader(self.options)

        # Register custom data source
        spark.dataSource.register(CustomIoTDataSource)
        print("[✓] Successfully registered 'custom_iot' Data Source!")

        # Query using standard spark.read.format()
        df = spark.read.format("custom_iot").option("total", "8").load()
        print("[✓] Querying custom registered Python Data Source:")
        df.show()
    else:
        # Fallback / Educational Demonstration of Connector Architecture
        print("\n[Step 2] Educational Demonstration of Custom Data Connector Architecture:")
        print("    Code pattern:")
        print("""
        from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

        class CustomDataSource(DataSource):
            @classmethod
            def name(cls): return "my_source"
            def schema(self): return StructType([...])
            def reader(self, schema): return MyReader(self.options)
        
        spark.dataSource.register(CustomDataSource)
        df = spark.read.format("my_source").load()
        """)
        
        # Emulate custom data ingestion using RDD parallel generator
        print("[✓] Emulating partition generation using parallelized worker generator:")
        partitions_data = [
            [(1, "Sensor_Alpha", 12.4), (2, "Sensor_Beta", 12.1)],
            [(3, "Sensor_Gamma", 12.5), (4, "Sensor_Delta", 12.8)]
        ]
        rdd = spark.sparkContext.parallelize(partitions_data, 2).flatMap(lambda x: x)
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("device_name", StringType(), False),
            StructField("voltage", DoubleType(), False)
        ])
        emulated_df = spark.createDataFrame(rdd, schema)
        emulated_df.show()

    print("\n[*] Module 35 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_35()

"""
## 5. Expected Output
=================================================================
 Module 35: Python Custom Data Source API (Pure Python Source)   
=================================================================

[*] Spark Runtime Version: 3.5.x
[✓] Native 'pyspark.sql.datasource' API is available in this Spark distribution.
[✓] Successfully registered 'custom_iot' Data Source!
[✓] Querying custom registered Python Data Source:
+---+---------------+-------+
| id|    device_name|voltage|
+---+---------------+-------+
|  0|Sensor_Device_0|    0.0|
|  1|Sensor_Device_1|   12.5|
|  2|Sensor_Device_2|   25.0|
|  3|Sensor_Device_3|   37.5|
|  4|Sensor_Device_4|   50.0|
|  5|Sensor_Device_5|   62.5|
|  6|Sensor_Device_6|   75.0|
|  7|Sensor_Device_7|   87.5|
+---+---------------+-------+

[*] Module 35 completed successfully.

## 6. Try It Yourself (Exercises)
1. Add a `DataSourceWriter` implementation to support `df.write.format("custom_iot").save()`.
2. Implement custom filter pushdown by adding `pushFilters()` to the `DataSourceReader`.
3. Build a custom data source that queries a public HTTP REST API in parallel across partitions.
"""

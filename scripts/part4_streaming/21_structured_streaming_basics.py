"""
# Module 21: Structured Streaming Basics (Rate & File Sources, Sinks, Output Modes, Triggers)

## 1. What It Is
**Structured Streaming** is a scalable and fault-tolerant stream processing engine built on top of the Spark SQL engine.
You express streaming computations the exact same way you express batch computations on static DataFrames.
Spark automatically converts the DataFrame query into an incremental execution plan that processes incoming data in low-latency micro-batches.

## 2. Why It Matters
Traditional stream processing systems (like Storm or Spark DStreams) required a completely different API, complex state management, and lacked end-to-end exactly-once guarantees.
Structured Streaming unifies batch and streaming APIs, ensuring that your batch transformations work identically in streaming with zero code rewrites.

## 3. Concept Diagram
```
  Unbounded Table Concept:
  Time   Input Stream (New Rows)          Unbounded Table (Stream DataFrame)
  t0     Row 1, Row 2            ──►      [Row 1, Row 2]
  t1     Row 3                   ──►      [Row 1, Row 2, Row 3]
  t2     Row 4, Row 5            ──►      [Row 1, Row 2, Row 3, Row 4, Row 5]
                                                    │
                                                    ▼
                                          Incremental Trigger
                                                    │
                                                    ▼
                                          Output Modes:
                                          • Append   (Only new rows)
                                          • Update   (Only changed rows)
                                          • Complete (Entire updated table)
```

## 7. Common Mistakes & Gotchas
- **Using unsupported operations in Append mode**: Aggregations without watermarks cannot run in `Append` mode because Spark does not know when an aggregation group is "finalized". You must use `Complete` or `Update` mode, or provide a watermark.
- **Forgetting awaitTermination()**: In a production standalone streaming script, omitting `query.awaitTermination()` causes the Python script to exit immediately after launching the background streaming query thread.
- **Not defining a checkpointLocation**: In production sinks (Kafka, S3, Delta Lake), running without `checkpointLocation` causes catastrophic data loss or duplicates when the driver restarts.

## 8. Official Documentation
- Structured Streaming Programming Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
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
from pyspark.sql.functions import col

def run_module_21():
    print("=================================================================")
    print(" Module 21: Structured Streaming Basics (Sources, Modes, Sinks)  ")
    print("=================================================================")

    spark = get_spark_session("Module21-StreamingBasics")

    # 1. Built-in Testing Source: 'rate' source (Generates sequential numbers with timestamps)
    print("\n[Step 1] Ingesting from streaming 'rate' source (5 rows per second)...")
    rate_stream = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", "5")
        .option("numPartitions", "1")
        .load()
    )
    print(f"[*] Stream isStreaming flag: {rate_stream.isStreaming}")

    # Transformation: Enrich streaming records
    transformed_stream = rate_stream.withColumn("even_or_odd", (col("value") % 2 == 0))

    # 2. Writing Stream to In-Memory Sink with Complete Mode
    print("\n[Step 2] Launching Streaming Query to Memory Sink (queryName='rate_counts')...")
    # Aggregation on stream
    counts_stream = transformed_stream.groupBy("even_or_odd").count()

    query_memory = (
        counts_stream.writeStream
        .queryName("rate_counts")
        .format("memory")
        .outputMode("complete") # Output entire aggregation table
        .trigger(processingTime="1 second") # 1-second micro-batch trigger
        .start()
    )

    print("[*] Stream running... Collecting 3 consecutive micro-batch results:")
    for i in range(1, 4):
        time.sleep(1.2)
        print(f"\n--- Batch Snapshot #{i} (Queried from in-memory table 'rate_counts') ---")
        spark.sql("SELECT * FROM rate_counts").show()

    # Query Status and Progress Inspection
    print("[*] Inspecting Query Status & Last Progress Metrics:")
    status = query_memory.status
    print(f"    - Is Active:               {query_memory.isActive}")
    print(f"    - Status Message:          {status['message']}")
    print(f"    - Is Data Available:       {status['isDataAvailable']}")
    print(f"    - Is Trigger Active:       {status['isTriggerActive']}")

    recent_prog = query_memory.lastProgress
    if recent_prog:
        print(f"    - Input Rows Per Second:   {recent_prog.get('inputRowsPerSecond', 0):.1f}")
        print(f"    - Process Rows Per Second: {recent_prog.get('processedRowsPerSecond', 0):.1f}")

    # Cleanly stop the query
    print("\n[Step 3] Stopping streaming query cleanly...")
    query_memory.stop()
    print("[✓] Streaming query stopped.")

    print("\n[*] Module 21 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_21()

"""
## 5. Expected Output
=================================================================
 Module 21: Structured Streaming Basics (Sources, Modes, Sinks)  
=================================================================

[Step 1] Ingesting from streaming 'rate' source (5 rows per second)...
[*] Stream isStreaming flag: True

[Step 2] Launching Streaming Query to Memory Sink (queryName='rate_counts')...
[*] Stream running... Collecting 3 consecutive micro-batch results:

--- Batch Snapshot #1 (Queried from in-memory table 'rate_counts') ---
+------------+-----+
|even_or_odd|count|
+------------+-----+
|        true|    ...|
|       false|    ...|
+------------+-----+
...

[Step 3] Stopping streaming query cleanly...
[✓] Streaming query stopped.

[*] Module 21 completed successfully.

## 6. Try It Yourself (Exercises)
1. Launch a query using `.trigger(availableNow=True)` (replaces the legacy `.trigger(once=True)`). How does it process available backlog data before shutting down?
2. Change the output sink to `.format("console")` with `.outputMode("append")` on the unaggregated `transformed_stream`.
3. Try running an aggregation in `outputMode("append")` without a watermark. What exact `AnalysisException` does Spark raise?
"""

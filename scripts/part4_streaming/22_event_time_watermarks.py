"""
# Module 22: Event-Time Processing, Window Aggregations & Watermarks

## 1. What It Is
**Event-Time** is the time embedded within the data record when the event actually occurred on the client device (e.g. click timestamp),
as opposed to **Processing-Time**, which is the clock time when the Spark executor receives the record.
A **Watermark** is a dynamic threshold defining how long Spark should wait for late-arriving data before dropping it and purging internal state memory.

## 2. Why It Matters
In real-world networks (mobile apps, IoT sensors), packets arrive out of order or hours late due to network dropouts.
Without watermarks, stateful aggregations (like 10-minute order summaries) would have to store state indefinitely in memory, eventually triggering an executor Out-Of-Memory crash. Watermarks bound state memory while correctly accounting for late events.

## 3. Concept Diagram
```
  Event Timeline:
  [Event: 10:00] ──► [Event: 10:05] ──► [Event: 10:15 (Max seen)]
                                                   │
  Watermark Delay: 10 minutes                      ▼
  Watermark Threshold = 10:15 - 10 min = 10:05

  • Record with timestamp 10:07 arrives ──► ACCEPTED (10:07 > 10:05) -> Updates window
  • Record with timestamp 10:02 arrives ──► DROPPED   (10:02 < 10:05) -> Expired, state purged!
```

## 7. Common Mistakes & Gotchas
- **Defining watermark after groupBy**: Watermarks must be defined **before** the aggregation (`df.withWatermark(...).groupBy(...)`). Calling `withWatermark()` after `groupBy()` has no effect.
- **Using processing time column for watermarks**: Watermarks only work on timestamp columns originating from the data source itself, not system clock functions like `current_timestamp()`.
- **Watermark delay too short**: Setting the delay threshold to 0 or 1 second will drop legitimate slightly out-of-order records during network hiccups.

## 8. Official Documentation
- Handling Late Data with Watermarks: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking
"""

import sys
import os
import time

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
from pyspark.sql.functions import col, window, count, expr

def run_module_22():
    print("=================================================================")
    print(" Module 22: Event-Time Processing, Windows & Watermarks          ")
    print("=================================================================")

    spark = get_spark_session("Module22-EventTimeWatermarks")

    # 1. Simulate Streaming Event Data with Timestamps
    print("\n[Step 1] Initializing streaming rate source with generated event timestamps...")
    # Generate timestamp stream where event_time advances sequentially
    stream_events = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .load()
        .withColumn("event_time", col("timestamp"))
        .withColumn("action_type", expr("CASE WHEN value % 3 = 0 THEN 'CLICK' WHEN value % 3 = 1 THEN 'VIEW' ELSE 'PURCHASE' END"))
    )

    # 2. Applying Watermark and Window Aggregation
    print("\n[Step 2] Defining 10-second Tumbling Window with 5-second Watermark Delay...")
    # withWatermark MUST be placed before groupBy
    windowed_stream = (
        stream_events
        .withWatermark("event_time", "5 seconds")
        .groupBy(
            window("event_time", "10 seconds", "5 seconds"), # 10s window sliding every 5s
            "action_type"
        )
        .agg(count("value").alias("action_count"))
    )

    # 3. Running Query in Update Mode
    print("\n[Step 3] Starting Query in 'update' Mode to Monitor State Evolution...")
    query = (
        windowed_stream.writeStream
        .queryName("windowed_actions")
        .format("memory")
        .outputMode("update") # Only outputs windows updated in the current micro-batch
        .trigger(processingTime="2 seconds")
        .start()
    )

    print("[*] Processing micro-batches... Sampling 3 intervals:")
    for i in range(1, 4):
        time.sleep(2.5)
        print(f"\n--- Sliding Window Update Snapshot #{i} ---")
        spark.sql("""
            SELECT 
                window.start, 
                window.end, 
                action_type, 
                action_count 
            FROM windowed_actions 
            ORDER BY window.start DESC, action_count DESC 
            LIMIT 5
        """).show(truncate=False)

    # 4. Check Watermark Progress Metadata
    print("\n[Step 4] Inspecting Event-Time Watermark Progression:")
    progress = query.lastProgress
    if progress:
        event_time_metrics = progress.get("eventTime", {})
        print(f"    - Watermark Timestamp:     {event_time_metrics.get('watermark')}")
        print(f"    - Max Event Timestamp:     {event_time_metrics.get('max')}")
        print(f"    - Min Event Timestamp:     {event_time_metrics.get('min')}")

    query.stop()
    print("\n[*] Module 22 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_22()

"""
## 5. Expected Output
=================================================================
 Module 22: Event-Time Processing, Windows & Watermarks          
=================================================================

[Step 1] Initializing streaming rate source with generated event timestamps...
[Step 2] Defining 10-second Tumbling Window with 5-second Watermark Delay...
[Step 3] Starting Query in 'update' Mode to Monitor State Evolution...

--- Sliding Window Update Snapshot #1 ---
+-------------------+-------------------+-----------+------------+
|start              |end                |action_type|action_count|
+-------------------+-------------------+-----------+------------+
|...                |...                |...        |...         |
+-------------------+-------------------+-----------+------------+

[Step 4] Inspecting Event-Time Watermark Progression:
    - Watermark Timestamp:     ...
    - Max Event Timestamp:     ...
    - Min Event Timestamp:     ...

[*] Module 22 completed successfully.

## 6. Try It Yourself (Exercises)
1. Contrast a 10-second Tumbling window (`window("event_time", "10 seconds")`) vs a 10-second window sliding every 2 seconds. How many windows does each row belong to in a sliding window?
2. Switch `outputMode` to `"append"` with the watermark. When does Spark output the finalized window record in Append mode?
3. What is the impact on state memory if you increase watermark delay from "5 seconds" to "24 hours"?
"""

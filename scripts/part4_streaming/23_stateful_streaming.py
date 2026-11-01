"""
# Module 23: Stateful Streaming Operations (Deduplication, Stream Joins, Custom State)

## 1. What It Is
Stateful stream processing maintains information across micro-batches to track historical context, detect duplicates, or compute multi-stream correlations.
Key stateful capabilities include:
- **Streaming dropDuplicates()**: Drops duplicate records within a watermark time window.
- **Stream-Static Joins**: Enriches real-time stream events with slowly changing dimension tables.
- **Stream-Stream Joins**: Joins two live independent streams with an event-time constraint.

## 2. Why It Matters
Real-world systems inevitably produce duplicate network events (e.g. mobile retries or Kafka at-least-once deliveries).
Without watermarked deduplication and stream joins, companies report inflated sales figures and false metric spikes.
Spark's built-in state store (HDFSBackedStateStore or RocksDBStateStore) manages internal state transparently.

## 3. Concept Diagram
```
  Stream-Static Join:
    Stream (Orders) ────► [ Hash Join ] ────► Enriched Output
    Static (Products) ──► (Cached in Executor Memory)

  Stream-Stream Join (Event-Time Window Match):
    Ad Impressions Stream ──┐ (Window: 0 to 15 min delay)
                            ├──► Match on 'ad_id' ──► Converted Click
    Ad Clicks Stream ───────┘
```

## 7. Common Mistakes & Gotchas
- **Streaming deduplication without a watermark**: Calling `stream.dropDuplicates(["event_id"])` without a watermark causes the state store to retain every ID forever, eventually causing executor memory exhaustion.
- **Stream-stream join without time bounds**: When joining two streams, Spark requires both a watermark AND an event-time range condition (e.g. `clicks.time >= impressions.time AND clicks.time <= impressions.time + INTERVAL 1 HOUR`).
- **State store memory limits**: For massive state stores (millions of keys), the default JVM memory state store causes GC pauses. Switch to the RocksDB state store provider (`spark.sql.streaming.stateStore.providerClass`).

## 8. Official Documentation
- Streaming Joins & Stateful Operations: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#streaming-joins
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
from pyspark.sql.functions import col, expr

def run_module_23():
    print("=================================================================")
    print(" Module 23: Stateful Streaming (Deduplication & Stream Joins)    ")
    print("=================================================================")

    spark = get_spark_session("Module23-StatefulStreaming")

    # 1. Streaming Deduplication with Watermark
    print("\n[Step 1] Watermarked Streaming Deduplication (dropDuplicates)...")
    # Stream simulating duplicate transaction IDs
    raw_stream = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", "5")
        .load()
        .withColumn("event_time", col("timestamp"))
        .withColumn("transaction_id", (col("value") % 5).cast("string")) # Creates repeating transaction IDs 0..4
    )

    # Deduplicate by transaction_id within a 10-second watermark window
    deduped_stream = (
        raw_stream
        .withWatermark("event_time", "10 seconds")
        .dropDuplicates(["transaction_id", "event_time"])
    )

    # 2. Stream-Static Join Demonstration
    print("\n[Step 2] Executing Stream-Static Join with Products Parquet...")
    # Load static reference dimension table
    static_products = spark.read.parquet("data/raw/products.parquet").select("product_id", "name", "category", "price")

    # Stream simulating incoming orders
    order_stream = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", "4")
        .load()
        .withColumn("order_id", col("value"))
        .withColumn("product_id", (col("value") % 20 + 1).cast("bigint")) # References products 1..20
    )

    # Perform stream-static inner join
    enriched_stream = order_stream.join(
        static_products,
        on="product_id",
        how="inner"
    )

    # 3. Launching Query to Memory Sink
    print("\n[Step 3] Starting Enriched Stream Query (queryName='enriched_orders')...")
    query = (
        enriched_stream.writeStream
        .queryName("enriched_orders")
        .format("memory")
        .outputMode("append")
        .start()
    )

    time.sleep(3.5)
    print("\n[✓] Snapshot of Real-time Enriched Orders (Stream + Static Joined):")
    spark.sql("SELECT order_id, product_id, name, category, price FROM enriched_orders LIMIT 5").show(truncate=False)

    query.stop()
    print("\n[*] Module 23 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_23()

"""
## 5. Expected Output
=================================================================
 Module 23: Stateful Streaming (Deduplication & Stream Joins)    
=================================================================

[Step 1] Watermarked Streaming Deduplication (dropDuplicates)...
[Step 2] Executing Stream-Static Join with Products Parquet...
[Step 3] Starting Enriched Stream Query (queryName='enriched_orders')...

[✓] Snapshot of Real-time Enriched Orders (Stream + Static Joined):
+--------+----------+-------------------+-----------+------+
|order_id|product_id|name               |category   |price |
+--------+----------+-------------------+-----------+------+
|...     |...       |...                |...        |...   |
+--------+----------+-------------------+-----------+------+

[*] Module 23 completed successfully.

## 6. Try It Yourself (Exercises)
1. Write a stream-to-stream outer join between an impressions stream and a clicks stream with an event-time boundary: `clicks.time BETWEEN impressions.time AND impressions.time + INTERVAL 15 MINUTES`.
2. Configure RocksDB state store by setting `spark.sql.streaming.stateStore.providerClass = org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider`.
3. In `dropDuplicates()`, what happens if you deduplicate on `["transaction_id"]` without including the watermark column?
"""

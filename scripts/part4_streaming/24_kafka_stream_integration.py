"""
# Module 24: Apache Kafka Streaming Integration & foreachBatch Custom Sinks

## 1. What It Is
Apache Spark provides built-in integration with Apache Kafka through the `spark-sql-kafka` connector.
It allows reading unbounded streams of Kafka topic partitions into streaming DataFrames and publishing transformed records back to Kafka.
The `foreachBatch()` operator provides an escape hatch to execute arbitrary batch DataFrame logic (such as `MERGE`, database upserts, or multi-sink writes) on each streaming micro-batch.

## 2. Why It Matters
Kafka is the enterprise standard for real-time messaging buses.
Structured Streaming guarantees **End-to-End Exactly-Once Processing** when paired with transactional Kafka producers and idempotent sinks.
`foreachBatch()` enables writing to traditional SQL databases, Delta Lake tables, or third-party APIs without sacrificing fault tolerance.

## 3. Concept Diagram
```
  Kafka Broker (Topic: 'clickstream_events')
  [Partition 0] [Partition 1] [Partition 2]
                     │
                     ▼
  Structured Streaming (readStream format("kafka"))
  Columns: [key, value, topic, partition, offset, timestamp]
                     │
                     ▼
  Transformations (from_json, filter, enrich)
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
  writeStream.format("kafka")    foreachBatch(micro_batch_sink)
  (Topic: 'alert_events')        (Idempotent Upsert to DB / Warehouse)
```

## 7. Common Mistakes & Gotchas
- **Forgetting to cast binary value**: Kafka stores message keys and values as raw byte arrays. In Spark, you must explicitly cast them using `col("value").cast("string")` before parsing with `from_json()`.
- **Checkpoint reuse across topics**: Changing the subscribed topic or Kafka cluster while keeping the same `checkpointLocation` corrupts consumer offset state. Clear the checkpoint directory when redefining Kafka subscriptions.
- **Side effects inside foreachBatch on retries**: If a micro-batch fails halfway, Spark re-executes `foreachBatch` with the same `batch_id`. Sinks must be **idempotent** (e.g. using `MERGE` or deduplication by `batch_id`).

## 8. Official Documentation
- Structured Streaming + Kafka Integration Guide: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
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
from pyspark.sql.functions import col, from_json, to_json, struct, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def run_module_24():
    print("=================================================================")
    print(" Module 24: Kafka Integration & foreachBatch Custom Sinks        ")
    print("=================================================================")

    spark = get_spark_session("Module24-KafkaStreaming")

    # 1. Defining Event Schema for Kafka JSON Payloads
    payload_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("action", StringType(), True)
    ])

    # 2. Simulating or Connecting to Kafka Source
    # We configure a stream that produces Kafka-identical binary schema: key (binary), value (binary)
    print("\n[Step 1] Constructing streaming source with Kafka-compliant schema...")
    raw_kafka_stream = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", "5")
        .load()
        .withColumn("key", col("value").cast("string").cast("binary"))
        .withColumn(
            "json_payload",
            to_json(struct(
                expr("concat('evt_', value)").alias("event_id"),
                (col("value") % 100 + 1).alias("customer_id"),
                expr("CASE WHEN value % 2 = 0 THEN 'ADD_TO_CART' ELSE 'SEARCH' END").alias("action")
            ))
        )
        .withColumn("value", col("json_payload").cast("binary"))
    )

    # 3. Parsing and Transforming Kafka Messages
    print("\n[Step 2] Deserializing Kafka Binary Payloads & Extracting Fields...")
    parsed_stream = (
        raw_kafka_stream
        .select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), payload_schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        )
        .select("kafka_key", "data.*", "kafka_timestamp")
    )

    # 4. Custom Sink with foreachBatch()
    print("\n[Step 3] Implementing Custom Database Sink with foreachBatch()...")
    sink_target_dir = "data/warehouse/module24_foreachbatch_sink"
    os.makedirs(sink_target_dir, exist_ok=True)

    def write_micro_batch_to_sink(batch_df, batch_id):
        """
        Custom sink executed for every micro-batch.
        Allows idempotent writes, ACID merges, or logging to monitoring services.
        """
        row_count = batch_df.count()
        print(f"    --> [Batch {batch_id}] Processing micro-batch with {row_count} records...")
        
        # In a real environment, you might run: batch_df.write.format("jdbc").mode("append").save()
        # Here we write to an append Parquet sink partitioned by action
        batch_df.write.mode("append").parquet(f"{sink_target_dir}/data")

    # Launch streaming query with foreachBatch
    query = (
        parsed_stream.writeStream
        .foreachBatch(write_micro_batch_to_sink)
        .trigger(processingTime="1 second")
        .start()
    )

    time.sleep(3.5)
    query.stop()
    print("[✓] Streaming query stopped.")

    # Verify persisted output
    persisted_count = spark.read.parquet(f"{sink_target_dir}/data").count()
    print(f"\n[Step 4] Verified Persisted Records in Custom Sink: {persisted_count} rows written successfully.")

    print("\n[*] Module 24 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_24()

"""
## 5. Expected Output
=================================================================
 Module 24: Kafka Integration & foreachBatch Custom Sinks        
=================================================================

[Step 1] Constructing streaming source with Kafka-compliant schema...
[Step 2] Deserializing Kafka Binary Payloads & Extracting Fields...
[Step 3] Implementing Custom Database Sink with foreachBatch()...
    --> [Batch 0] Processing micro-batch with ... records...
    --> [Batch 1] Processing micro-batch with ... records...
[✓] Streaming query stopped.

[Step 4] Verified Persisted Records in Custom Sink: ... rows written successfully.

[*] Module 24 completed successfully.

## 6. Try It Yourself (Exercises)
1. Start the Kafka container via `docker compose up -d kafka`. Run a producer that sends JSON events to topic `ecommerce_clicks`.
2. Connect Spark to the live container using `.option("kafka.bootstrap.servers", "localhost:29092").option("subscribe", "ecommerce_clicks")`.
3. Inside `foreachBatch`, implement an upsert (MERGE) operation to update a customer's `last_active_time` idempotently.
"""

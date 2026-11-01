# Part 5 Solutions (Structured Streaming: Modules 21 - 25)

## Module 21: Structured Streaming Basics
1. **trigger(availableNow=True)**:
   *Insight*: `availableNow=True` processes all pending backlog data across multiple micro-batches until the stream has caught up with the source, then cleanly terminates. This allows running streaming pipelines as cost-effective scheduled batch cron jobs.
2. **Console Sink in Append Mode**:
   ```python
   query = transformed_stream.writeStream.format("console").outputMode("append").start()
   ```
3. **Aggregation in Append Mode Error**:
   *Insight*: Raises `AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark`. In Append mode, Spark must know when an aggregation is finalized before emitting a row.

---

## Module 22: Event-Time Processing & Watermarks
1. **Tumbling vs Sliding Windows**:
   *Insight*: In a 10s window sliding every 2s, each event row belongs to 5 overlapping windows (`10 / 2 = 5`). In a tumbling window, each event belongs to exactly 1 window.
2. **Append Mode Output Timing with Watermark**:
   *Insight*: In Append mode with watermarks, a window's aggregate row is only emitted to the sink *after* the watermark has moved past the window's end time (`watermark >= window.end`), ensuring no further late events will alter that window.
3. **State Memory Impact of Large Watermark Delay**:
   *Insight*: Increasing the watermark delay to 24 hours forces Spark to retain 24 hours worth of state keys and partial aggregates in executor RAM/state store, exponentially increasing state memory consumption.

---

## Module 23: Stateful Streaming & Joins
1. **Stream-to-Stream Join with Time Boundary**:
   ```python
   joined = impressions.join(
       clicks,
       expr("""
           clicks.ad_id = impressions.ad_id AND
           clicks.click_time >= impressions.imp_time AND
           clicks.click_time <= impressions.imp_time + interval 15 minutes
       """),
       how="leftOuter"
   )
   ```
2. **Configuring RocksDB State Store**:
   ```python
   spark.conf.set(
       "spark.sql.streaming.stateStore.providerClass",
       "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
   )
   ```
   *Insight*: RocksDB spills off-heap to local SSD, supporting hundreds of millions of state keys without JVM GC pauses.
3. **dropDuplicates() without Watermark**:
   *Insight*: Without a watermark, the state store retains every historical `transaction_id` indefinitely, leading to unbounded state growth and eventual Out-Of-Memory.

---

## Module 24: Kafka & foreachBatch
1. **Kafka Container Setup**:
   *Insight*: Run `docker compose up -d kafka` to start Kafka at `localhost:29092` with Zookeeper.
2. **Kafka Subscription**:
   ```python
   df = (
       spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "localhost:29092")
       .option("subscribe", "ecommerce_clicks")
       .load()
   )
   ```
3. **Idempotent Upsert in foreachBatch**:
   *Insight*: Use `MERGE INTO` or target table deduplication by `batch_id` to guarantee that micro-batch re-executions (e.g. after worker crash) do not insert duplicate records.

---

## Module 25: Checkpointing & Crash Recovery
1. **Offset File Structure**:
   *Insight*: An offset file contains JSON metadata with the source URI and the exact partition offsets (e.g. `{"topic_name": {"0": 1045, "1": 982}}`).
2. **Uncommitted Batch Recovery**:
   *Insight*: If `offsets/1` exists but `commits/1` is missing, Spark assumes batch 1 failed midway during write. On restart, Spark discards partial sink output (if idempotent) and re-executes batch 1 using the logged offsets.
3. **Schema Mutation across Checkpoints**:
   *Insight*: Adding a column without an evolution mechanism throws `AnalysisException` because checkpointed state schemas are immutable.

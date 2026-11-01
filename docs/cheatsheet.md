# Apache Spark Masterclass: One-Page Cheatsheet

A concise, high-density reference summarizing core concepts, APIs, performance rules, and code patterns across all 41 modules.

---

## 1. Spark Architecture & Core Rules
- **Driver**: Maintains `SparkSession`, creates the Execution DAG, coordinates worker task scheduling.
- **Executors**: Worker JVM processes executing tasks and caching partitions.
- **Transformations (Lazy)**: `map`, `filter`, `select`, `groupBy`, `join` (builds logical lineage; zero execution).
- **Actions (Eager)**: `count()`, `collect()`, `take()`, `first()`, `write`, `saveAsTable()` (submits DAG jobs).
- **Narrow vs Wide**: Narrow dependencies (e.g. `map`, `filter`) keep execution in the same partition. Wide dependencies (e.g. `groupBy`, `join`, `distinct`) require network shuffle exchanges across nodes.

---

## 2. DataFrame & SQL Essentials
```python
# Session & Schema
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("App").master("local[*]").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Read & Write
df = spark.read.option("header", "true").schema(schema).csv("data.csv")
df.write.mode("overwrite").partitionBy("category").parquet("out_dir")

# Essential Column Transformations
df = (
    df.withColumn("tax", round(col("amount") * 0.10, 2))
    .withColumnRenamed("name", "full_name")
    .filter((col("tax") > 5.0) & col("full_name").isNotNull())
    .dropDuplicates(["id"])
    .na.fill({"tax": 0.0})
)

# Views & SQL Interoperability
df.createOrReplaceTempView("v_items")
spark.sql("SELECT category, round(avg(amount), 2) AS mean_amt FROM v_items GROUP BY category")
```

---

## 3. Joins & Physical Strategies
- **Inner / Left / Right / Full**: `df1.join(df2, on="key", how="left")`
- **Left Semi**: `df1.join(df2, on="key", how="left_semi")` (Returns left rows having a match in right).
- **Left Anti**: `df1.join(df2, on="key", how="left_anti")` (Returns left rows having NO match in right).
- **Broadcast Hash Join (BHJ)**: `df1.join(broadcast(small_df), on="key")` (Eliminates network shuffle).
- **Sort Merge Join (SMJ)**: Standard default for large-to-large distributed tables.

---

## 4. Window Functions & Frame Specifications
```python
from pyspark.sql.window import Window

# Partitioned Ranking
w_rank = Window.partitionBy("category").orderBy(col("price").desc())
df.withColumn("rn", row_number().over(w_rank)).filter(col("rn") <= 3)

# Running Cumulative Sum & Moving Average
w_cum = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_mov = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(-2, Window.currentRow)

df.withColumn("running_total", sum("amount").over(w_cum)).withColumn("moving_avg_3", avg("amount").over(w_mov))
```

---

## 5. Performance Tuning & Skew Mitigation
- **The 5-Core Rule**: Allocate **4 to 5 cores per executor** (`--executor-cores 4`) to balance task parallelism without JVM garbage collection pauses.
- **Executor Memory**: Target 16GB–32GB RAM per executor. Set `spark.executor.memoryOverhead` to `max(384MB, 0.10 * executorMemory)`.
- **Shuffle Partitions**: Tune `spark.sql.shuffle.partitions` (aim for 100MB–200MB uncompressed per partition).
- **Adaptive Query Execution (AQE)**: Always keep `spark.sql.adaptive.enabled = true` for dynamic partition coalescing, runtime join switching, and automatic skew mitigation.
- **Salting for Skewed Joins**: Append `floor(rand() * 4)` to the skewed key on the large table, and explode the lookup table across all possible salt values `[0, 1, 2, 3]`.

---

## 6. Structured Streaming & Fault Tolerance
```python
# Streaming Read with Watermark
stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events")
    .load()
    .withColumn("event_time", col("timestamp"))
    .withWatermark("event_time", "10 minutes")
    .groupBy(window("event_time", "15 minutes", "5 minutes"), "key")
    .count()
)

# Fault-Tolerant Sink with Dedicated Checkpoint
query = (
    stream_df.writeStream
    .format("parquet")
    .option("path", "s3://lakehouse/gold_stream/")
    .option("checkpointLocation", "s3://lakehouse/checkpoints/gold_stream/")
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)
query.awaitTermination()
```

---

## 7. Machine Learning (MLlib Pipelines)
```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression

indexer = StringIndexer(inputCol="tier", outputCol="tier_idx")
encoder = OneHotEncoder(inputCol="tier_idx", outputCol="tier_vec")
assembler = VectorAssembler(inputCols=["spend", "orders", "tier_vec"], outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label")

pipeline = Pipeline(stages=[indexer, encoder, assembler, scaler, lr])
model = pipeline.fit(train_df)
predictions = model.transform(test_df)
```

---

## 8. Open Table Formats (Delta Lake ACID)
```python
# Time Travel & MERGE
from delta.tables import DeltaTable

# Time Travel
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)

# Atomic Upsert (MERGE)
delta_tbl = DeltaTable.forPath(spark, delta_path)
(
    delta_tbl.alias("t")
    .merge(updates_df.alias("s"), "t.id = s.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
```

---

## 9. Top 5 Anti-Patterns to Avoid
1. ❌ Calling `.collect()` or `.count()` inside Python loops (causes driver Out-Of-Memory).
2. ❌ Writing Python UDFs when built-in functions in `pyspark.sql.functions` exist.
3. ❌ Running `repartition()` immediately followed by `coalesce()` (wastes full network shuffle).
4. ❌ Storing streaming checkpoints on ephemeral local disks (`/tmp`).
5. ❌ Hardcoding AWS/GCS access keys in Python source code (use IAM Roles / Vault).

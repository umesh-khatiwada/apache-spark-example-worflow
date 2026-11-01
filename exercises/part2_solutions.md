# Part 2 Solutions (Data Transformation: Modules 06 - 10)

## Module 06: Dataset API Guide & Scala
1. **spark.implicits._ in Scala**:
   *Insight*: Provides implicit encoders for standard Scala types (Int, String, Case Classes) that serialize JVM objects into Tungsten binary rows.
2. **AnalysisException Timing**:
   *Insight*: In PySpark, attribute typos in column references (e.g. `col("wrong_name")`) are caught when Catalyst analyzes the plan (upon defining the transformation or calling an action), rather than at Python syntax parse time.
3. **Memory Footprint**:
   *Insight*: A JVM object has object header overhead (16 bytes), reference pointers (8 bytes), and memory fragmentation. Tungsten UnsafeRows are tightly packed contiguous bytes with 1-byte null bitmasks and zero GC overhead.

---

## Module 07: I/O Formats, Write Modes & Parsing
1. **DROPMALFORMED Mode**:
   ```python
   df = spark.read.option("header", "true").option("mode", "DROPMALFORMED").schema(resilient_schema).csv(corrupt_data_dir)
   # Corrupt rows are silently dropped; output contains only the 2 valid records.
   ```
2. **FAILFAST Mode**:
   ```python
   df = spark.read.option("header", "true").option("mode", "FAILFAST").schema(resilient_schema).csv(corrupt_data_dir)
   # Calling df.count() throws org.apache.spark.SparkException: Malformed records detected in record parsing.
   ```
3. **Compression Ratio**:
   *Insight*: Parquet achieves between 4x to 8x compression compared to JSON due to column dictionary encoding and Snappy compression.

---

## Module 08: Joins & Join Strategies
1. **Cartesian Product Error**:
   *Insight*: Throws `AnalysisException: Detected implicit cartesian product for INNER join between logical plans... To enable it, set spark.sql.crossJoin.enabled=true`.
2. **Broadcast Speedup**:
   *Insight*: Broadcasting the smaller table eliminates the expensive shuffle exchange phase, typically reducing join runtime by 2x to 10x depending on cluster size and network bandwidth.
3. **AQE Dynamic Join Switching**:
   *Insight*: During stage execution, AQE observes runtime partition sizes. If one table turns out to be smaller than `spark.sql.adaptive.autoBroadcastJoinThreshold`, AQE dynamically rewires the plan from Sort-Merge Join to Broadcast Hash Join.

---

## Module 09: Aggregations & Window Functions
1. **Percentage of Category Total**:
   ```python
   cat_window = Window.partitionBy("category")
   df_with_pct = df_products.withColumn(
       "category_pct",
       round(col("price") / sum("price").over(cat_window) * 100, 2)
   )
   ```
2. **Days Elapsed to Next Order**:
   ```python
   from pyspark.sql.functions import datediff, to_date
   df_next = df_orders.withColumn(
       "days_to_next_order",
       datediff(lead(to_date("order_date"), 1).over(cust_order_window), to_date("order_date"))
   )
   ```
3. **rowsBetween vs rangeBetween**:
   *Insight*: `rowsBetween(-1, 1)` always takes exactly 1 row before and 1 row after regardless of timestamp value. `rangeBetween(-86400, 86400)` considers all rows whose timestamp falls within +/- 24 hours of the current row's timestamp value.

---

## Module 10: Complex Types & Nested JSON
1. **schema_of_json**:
   ```python
   from pyspark.sql.functions import schema_of_json, lit
   schema_str = spark.range(1).select(schema_of_json(lit('{"user": "alice", "score": 98.5}'))).first()[0]
   # Returns: struct<score:double,user:string>
   ```
2. **map_from_entries**:
   ```python
   # Grouping and aggregating into a map
   from pyspark.sql.functions import map_from_entries, collect_list, struct
   ```
3. **Array transformation with transform()**:
   ```python
   from pyspark.sql.functions import transform, upper
   df_upper = customer_history.withColumn("all_events_upper", transform("all_events", lambda x: upper(x)))
   ```

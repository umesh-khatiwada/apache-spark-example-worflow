# Part 1 Solutions (Foundations: Modules 01 - 05)

## Module 01: Spark Architecture & DAG
1. **Adding CA Filter**:
   ```python
   df_ca_gold = df_selected.filter(df_raw.state == "CA")
   df_ca_gold.explain()
   ```
   *Insight*: Catalyst merges adjacent filter operations into a single combined predicate `(isnotnull(loyalty_tier) AND (loyalty_tier = 'Gold') AND (state = 'CA'))` in the physical plan.
2. **Number of Partitions**:
   *Insight*: Spark splits file input by `spark.sql.files.maxPartitionBytes` (default 128 MB). For small CSVs, it creates 1 partition per input file unless partitioned or explicitly set.
3. **take(5) vs collect()**:
   *Insight*: `.take(5)` scans only the first partition(s) until 5 records are found and transmits only 5 records to the driver. `.collect()` scans all partitions across all executors and pulls the entire dataset across the network into driver RAM, risking an OutOfMemory error.

---

## Module 02: SparkSession & Configuration
1. **Modifiability Check**:
   ```python
   print(spark.conf.isModifiable("spark.driver.memory")) # Returns False
   ```
   *Insight*: Static JVM and infrastructure properties cannot be changed once the JVM is initialized.
2. **Session Isolation**:
   ```python
   session2 = spark.newSession()
   session2.conf.set("spark.sql.shuffle.partitions", "20")
   print(spark.conf.get("spark.sql.shuffle.partitions"))    # Remains 8
   print(session2.conf.get("spark.sql.shuffle.partitions")) # 20
   ```
   *Insight*: `newSession()` isolates runtime configuration, temporary views, and catalogs while sharing the underlying `SparkContext` and cluster resources.
3. **Catalog Listing**:
   ```python
   spark.catalog.listTables()
   ```
   *Insight*: Reports `isTemporary=True` and `database=None` for session-scoped temporary views.

---

## Module 03: RDD API Fundamentals
1. **aggregateByKey for Average Rating**:
   ```python
   # Value is (sum, count)
   zero_val = (0.0, 0)
   seq_op = lambda acc, rating: (acc[0] + rating, acc[1] + 1)
   comb_op = lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
   prod_ratings = (
       parsed_rdd.map(lambda c: (int(c[1]), float(c[3])))
       .aggregateByKey(zero_val, seq_op, comb_op)
       .mapValues(lambda totals: round(totals[0] / totals[1], 2))
   )
   ```
2. **toDebugString Inspection**:
   *Insight*: Look for `ShuffledRDD` or `MapPartitionsRDD` at the boundary between stages.
3. **distinct() Mechanics**:
   *Insight*: `distinct()` is a wide transformation. It converts elements to `(element, None)` pairs and executes `reduceByKey()`, triggering a full network shuffle.

---

## Module 04: DataFrame API Basics
1. **Conditional Column**:
   ```python
   from pyspark.sql.functions import when, col
   df_tier = df_enriched.withColumn(
       "spending_tier",
       when(col("net_amount") > 200, "High")
       .when(col("net_amount") > 50, "Medium")
       .otherwise("Low")
   )
   ```
2. **Null / Malformed Email Filter**:
   ```python
   df_bad_emails = df_customers.filter(col("email").isNull() | (~col("email").contains("@")))
   ```
3. **na.drop Effect**:
   ```python
   before = df_items.count()
   after = df_items.na.drop(subset=["discount"]).count()
   print(f"Dropped {before - after} rows with null discounts.")
   ```

---

## Module 05: Spark SQL & Views
1. **CASE WHEN Query**:
   ```sql
   SELECT order_id, total_amount,
       CASE 
           WHEN total_amount < 50 THEN 'Small'
           WHEN total_amount <= 200 THEN 'Medium'
           ELSE 'Large'
       END AS order_bracket
   FROM v_orders;
   ```
2. **Session View Isolation**:
   *Insight*: A new session cannot access local temp view `v_orders` (throws `TABLE_OR_VIEW_NOT_FOUND`), but can access `global_temp.gv_customers` because it is scoped globally across all sessions.
3. **Dropping Temp View**:
   ```python
   spark.catalog.dropTempView("v_orders")
   # Subsequent query on v_orders raises AnalysisException: Table or view not found: v_orders
   ```

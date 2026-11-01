# Part 7 Solutions (Modern Spark Ecosystem: Modules 31 - 35)

## Module 31: pandas API on Spark
1. **Pandas Scalability**:
   *Insight*: Standard pandas loads the entire dataset into memory and processes it on a single CPU core, failing with out-of-memory errors on multi-gigabyte data. `pyspark.pandas` partitions the data across cluster cores, achieving horizontal scalability.
2. **Index Type Trade-offs**:
   *Insight*: `"sequence"` creates an expensive global window across all partitions to generate strict incremental 0, 1, 2... integers. `"distributed"` generates non-contiguous partition-based IDs with zero shuffle overhead.
3. **transform_batch()**:
   *Insight*: Allows applying vectorized pandas or scikit-learn batch functions on chunks of distributed data without converting the entire table to a local pandas DataFrame.

---

## Module 32: Spark Connect
1. **Starting Connect Server**:
   ```bash
   /opt/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.3
   ```
2. **Thin Client Isolation**:
   *Insight*: Because the client sends Protocol Buffer messages over gRPC, client machines require only `pip install pyspark[connect]` and do not need Java installed.
3. **Cost Optimization Location**:
   *Insight*: The client creates an unresolved logical plan. The plan is transmitted over gRPC to the server, where Catalyst's parser, analyzer, optimizer, and physical planner execute remotely.

---

## Module 33: Open Table Formats & Delta Lake
1. **Schema Evolution with mergeSchema**:
   ```python
   df.write.format("delta").mode("append").option("mergeSchema", "true").save(table_path)
   ```
   *Insight*: Delta Lake automatically adds the new column to the table metadata schema without requiring existing data files to be rewritten.
2. **VACUUM Retention**:
   *Insight*: `VACUUM` permanently deletes raw Parquet data files that are no longer referenced in the transaction log and are older than the retention threshold (default 7 days).
3. **Delta vs Iceberg Architecture**:
   *Insight*: Delta Lake uses an append-only JSON transaction log (`_delta_log/`) with periodic checkpoint Parquet files. Apache Iceberg uses hierarchical snapshot trees (Snapshot -> Manifest List -> Manifest File -> Data Files).

---

## Module 34: Declarative Pipelines & Data Quality
1. **Email Quality Rule**:
   ```python
   table = DeclarativeTable(
       name="clean_customers",
       query_fn=lambda spk: spk.read.csv("data/raw/customers.csv"),
       expectations=[("email LIKE '%@%.%'", "DROP")]
   )
   ```
2. **ON VIOLATION FAIL**:
   *Insight*: Immediately halts pipeline execution and aborts transaction commits, ensuring corrupted data never pollutes downstream analytical tables.
3. **DLT CDC (Change Data Capture)**:
   *Insight*: Delta Live Tables provides `APPLY CHANGES INTO`, which automatically handles out-of-order inserts, updates, and deletes from operational databases into a clean SCD Type 1 or Type 2 table.

---

## Module 35: Python Custom Data Source API
1. **DataSourceWriter**:
   *Insight*: Implements `write(iterator)` on worker tasks to serialize Spark rows into proprietary formats or stream to remote APIs.
2. **Filter Pushdown**:
   *Insight*: Implementing `pushFilters(filters)` allows the custom reader to inspect Catalyst filter expressions and skip partitions at the source level.
3. **HTTP REST Partitioning**:
   *Insight*: The `partitions()` method can calculate API page ranges (e.g. `page=1..10`, `page=11..20`), allowing multiple worker tasks to query the HTTP API concurrently.

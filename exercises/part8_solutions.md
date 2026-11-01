# Part 8 Solutions (Production & Architecture: Modules 36 - 40)

## Module 36: Catalog API & Metastore
1. **MSCK REPAIR TABLE**:
   ```sql
   MSCK REPAIR TABLE external_customers;
   ```
   *Insight*: Scans the external storage directory for newly added partition folders (e.g. `state=CA/`) and updates the Hive Metastore catalog automatically.
2. **List Columns Programmatically**:
   ```python
   columns = spark.catalog.listColumns("external_customers")
   for col_meta in columns:
       print(col_meta.name, col_meta.dataType, col_meta.isNullable)
   ```
3. **Table Caching via Catalog**:
   ```python
   spark.catalog.cacheTable("managed_customers")
   print(spark.catalog.isCached("managed_customers")) # True
   ```

---

## Module 37: Deployment Modes & spark-submit
1. **Multipass Cluster Submission**:
   *Insight*: The script `./scripts/run_on_cluster.sh` transfers the python script to the Multipass master node and executes `spark-submit --master spark://<master_ip>:7077 --deploy-mode client`.
2. **Cluster Mode on Local Master**:
   *Insight*: Running `spark-submit --master local[*] --deploy-mode cluster` raises `IllegalArgumentException: Cluster deploy mode is not supported by master 'local[*]'.` Local master requires client mode.
3. **Dynamic Resource Allocation**:
   *Insight*: With dynamic allocation enabled, Spark monitors the task backlog. If tasks wait in queue longer than `spark.dynamicAllocation.schedulerBacklogTimeout` (1s), it requests new executors up to `maxExecutors`. If an executor is idle for `executorIdleTimeout` (60s), it is decommissioned to save resources.

---

## Module 38: Testing Spark Code with Pytest
1. **Testing Negative Balances**:
   ```python
   def test_negative_refunds(spark):
       refund_orders = spark.createDataFrame([(1, 1, -100.0)], ["order_id", "customer_id", "total_amount"])
       res = calculate_loyalty_rewards(mock_customers.limit(1), refund_orders).first()
       assert res.total_spend == -100.0
   ```
2. **assertDataFrameEqual**:
   ```python
   from pyspark.testing import assertDataFrameEqual
   assertDataFrameEqual(actual_df, expected_df, checkRowOrder=False)
   ```
3. **Temporary Test Directory Fixture**:
   ```python
   import tempfile, shutil
   @pytest.fixture
   def temp_dir():
       path = tempfile.mkdtemp()
       yield path
       shutil.rmtree(path)
   ```

---

## Module 39: Monitoring, Logging & History Server
1. **Custom SparkListener**:
   *Insight*: In PySpark, query status is monitored via `statusTracker()` or `StreamingQueryListener`, which receive callbacks on `onQueryStarted`, `onQueryProgress`, and `onQueryTerminated`.
2. **History Server Cleaner**:
   *Insight*: `spark.history.fs.cleaner.enabled=true` automatically deletes event log files older than `spark.history.fs.cleaner.maxAge` (e.g. 7d), preventing disk exhaustion.
3. **GC Time in SparkListenerTaskEnd**:
   *Insight*: Inside `"Task Metrics" -> "JVM GC Time"`, Spark records the exact milliseconds the executor JVM spent paused during garbage collection cycles.

---

## Module 40: Security, PII Masking & Anti-Patterns
1. **Credit Card Masking**:
   ```python
   from pyspark.sql.functions import concat, lit, col
   df_masked = df.withColumn("masked_cc", concat(lit("****-****-****-"), col("card_number").substr(-4, 4)))
   ```
2. **Kubernetes IRSA (IAM Roles for Service Accounts)**:
   *Insight*: The Kubernetes pod assumes an AWS IAM role via an OIDC token projected into the pod filesystem. Spark's AWS Hadoop connector exchanges this token for temporary STS credentials automatically, requiring zero static credentials in code.
3. **In-Transit RPC Encryption**:
   *Insight*: Enabling `spark.network.crypto.enabled=true` with SASL authentication encrypts all RPC data exchanges between driver and executors using AES-128/256, protecting against network snooping on shared infrastructure.

# Part 4 Solutions (Performance & Internals: Modules 16 - 20)

## Module 16: Caching & Persistence
1. **MEMORY_ONLY vs MEMORY_ONLY_SER**:
   *Insight*: `MEMORY_ONLY_SER` stores data as serialized byte arrays in JVM memory. It consumes 2x-5x less RAM and reduces GC pressure, but requires CPU cycles to deserialize objects when queried.
2. **Spark UI Storage Inspection**:
   *Insight*: Inspect `http://localhost:4040/storage/` to see "Size in Memory", "Size on Disk", and confirm "Fraction Cached" is 100%.
3. **blocking Parameter in unpersist()**:
   *Insight*: `blocking=True` blocks the Python thread until all executor memory blocks are completely freed from JVM memory before returning. `blocking=False` sends an asynchronous eviction command and returns immediately.

---

## Module 17: Partitioning, Coalesce & Bucketing
1. **Inspecting Partition Row Distribution**:
   ```python
   from pyspark.sql.functions import spark_partition_id
   df.groupBy(spark_partition_id().alias("pid")).count().show()
   ```
2. **Bucketing Join Advantage**:
   *Insight*: Joining two tables bucketed on the same join key into the same number of buckets eliminates the `Exchange hashpartitioning` shuffle operator entirely, converting the join into a fast localized merge.
3. **repartition(10) vs repartition(10, "col")**:
   *Insight*: `repartition(10)` performs round-robin partitioning, ensuring equal row counts per partition. `repartition(10, "col")` applies MurmurHash3 to the column value, ensuring identical values reside in the exact same partition.

---

## Module 18: Shared Variables
1. **Custom Set Accumulator**:
   ```python
   from pyspark.accumulators import AccumulatorParam
   class SetAccumulatorParam(AccumulatorParam):
       def zero(self, value):
           return set()
       def addInPlace(self, v1, v2):
           return v1.union(v2)
   ```
2. **unpersist() vs destroy()**:
   *Insight*: `unpersist()` deletes cached blocks from worker memory but allows the broadcast variable to be re-broadcasted later. `destroy()` completely destroys the broadcast ID across both workers and driver.
3. **Worker Mutation Side-Effect**:
   *Insight*: Mutating broadcast values on workers (`bc.value['key'] = 'val'`) modifies only the local Python process's copy. The mutation is NOT synchronized back to the driver or other executors.

---

## Module 19: Tuning & Salting
1. **Unbalanced Partition Detection**:
   *Insight*: An un-salted skewed DataFrame will show 85% of records grouped in partition 0, while partitions 1, 2, and 3 hold only 5% each.
2. **Salting Scalability**:
   *Insight*: Under large data volumes, an un-salted skewed partition spills gigabytes to disk, causing thrashing. Salted keys distribute data evenly across all executors, eliminating spill completely.
3. **AQE Skew Join Automation**:
   *Insight*: When AQE is enabled, Spark detects skewed partitions at the shuffle stage and automatically splits the skewed partition into smaller sub-partitions, joining them against replicated matching partitions without manual code changes.

---

## Module 20: Spark UI Diagnostics
1. **Triggering Spill**:
   *Insight*: Setting low executor memory causes Spark sort buffers to exceed RAM limits, displaying `Spill (Memory)` and `Spill (Disk)` in the Stages tab.
2. **HashAggregate Output Rows**:
   *Insight*: Opening the SQL DAG reveals exact row counts passing through every operator box.
3. **High Deserialization Time**:
   *Insight*: High `Task Deserialization Time` indicates large function closures being shipped from the driver, or excessive Python-JVM object serialization.

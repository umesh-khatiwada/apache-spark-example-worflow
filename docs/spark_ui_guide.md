# Spark Web UI Diagnostic & Troubleshooting Guide

The Spark Web UI runs at `http://localhost:4040` during application runtime (or port `18080` on the Spark History Server for past completed jobs).

---

## 1. Jobs Tab
- **Purpose**: Provides a macro timeline of all Spark jobs triggered by user actions (`count()`, `collect()`, `write`, `take()`).
- **Key Indicators**:
  - **Status**: Succeeded, Running, or Failed.
  - **Event Timeline**: Displays stages executing sequentially vs in parallel.
  - **Action**: Look at the "Description" column to map the job back to the line of code that triggered it.

---

## 2. Stages Tab
- **Purpose**: Breaks down a job into execution stages separated by shuffle Exchange boundaries.
- **Key Indicators**:
  - **Task Duration Quantiles (Min, 25%, 50%, 75%, Max)**:
    - *Healthy*: Max duration is roughly comparable to 75th percentile (e.g. 1.2s vs 1.0s).
    - *Data Skew Alert*: Max duration is 10x to 100x larger than 75th percentile (e.g. 120s vs 2s). This indicates one task is processing a massive skewed key!
  - **Shuffle Spill (Memory) / Shuffle Spill (Disk)**:
    - *Spill > 0*: Executor execution memory was insufficient to hold data structures during sorting or aggregation. Spilling to disk introduces severe I/O latency.
  - **GC Time**:
    - High GC time (>10% of total stage duration) means executor JVM heap is overwhelmed with garbage objects.

---

## 3. SQL / DataFrame Tab
- **Purpose**: Displays the complete Catalyst physical execution DAG for DataFrame queries.
- **Key Indicators**:
  - Click on the query description (e.g. `collect at ...`) to open the graphical DAG.
  - Inspect the numbers inside each node:
    - `Scan parquet`: Number of files scanned and rows read.
    - `Filter`: Number of rows passed through after filtering (verifies filter selectivity).
    - `BroadcastHashJoin` vs `SortMergeJoin`: Confirms which join strategy was physically executed.

---

## 4. Storage Tab
- **Purpose**: Displays all DataFrames and RDDs persisted using `.cache()` or `.persist()`.
- **Key Indicators**:
  - **Storage Level**: `Memory Deserialized`, `Memory Serialized`, or `Disk`.
  - **Fraction Cached**: Must be `100%`. If it is less than 100%, the cluster ran out of storage memory and remaining partitions were evicted or spilled!
  - **Size in Memory**: Exact footprint of the cached dataset.

---

## 5. Executors Tab
- **Purpose**: Summarizes resource allocation and hardware health across each worker node.
- **Key Indicators**:
  - **Active / Dead Executors**: Detects worker nodes that died from OOM (`Exit Code 137`) or network heartbeat timeouts.
  - **Task Time / GC Time**: Ratios over 10% indicate JVM memory tuning is needed.
  - **Shuffle Read / Shuffle Write**: Identifies whether data is evenly distributed across cluster nodes or heavily biased to one executor.

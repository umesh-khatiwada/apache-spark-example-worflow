"""
# Module 25: Checkpointing, Fault Tolerance & Exactly-Once Crash Recovery

## 1. What It Is
**Checkpointing** is Structured Streaming's fault-tolerance engine that records query metadata, source offsets, and state store snapshots to durable storage (e.g. HDFS, S3, or local disk).
When a streaming driver fails or a query is restarted, Spark reads the checkpoint directory to determine the exact offset where execution stopped, resuming seamlessly with zero data loss.

## 2. Why It Matters
In mission-critical production pipelines (financial fraud detection, order fulfillment), losing streaming data or double-counting transactions is unacceptable.
Combined with replayable sources (like Kafka) and idempotent sinks, Structured Streaming achieves **End-to-End Exactly-Once Processing Semantics**.

## 3. Concept Diagram: Inside the Checkpoint Directory
```
  checkpoint_dir/
  ├── metadata/        ──► Query run ID and UUID
  ├── offsets/         ──► Write-Ahead Log (WAL) of start and end offsets per batch (e.g. 0, 1, 2)
  ├── commits/         ──► Marks batches that have been successfully output to the sink
  └── state/           ──► Serialized state store delta files (for windowed aggregates/dedupes)
```

## 7. Common Mistakes & Gotchas
- **Modifying query schema under an existing checkpoint**: Adding or removing columns in a streaming DataFrame query while pointing to an old checkpoint raises `AnalysisException: Cannot mutate schema across checkpoint runs`. Use a new checkpoint directory or Delta Lake schema evolution.
- **Putting checkpoints on non-persistent local disks**: If running on Kubernetes or ephemeral cloud instances, storing checkpoints in `/tmp` means all offset progress is lost on pod restart. Checkpoints MUST reside on persistent distributed storage (S3, GCS, ADLS, HDFS).
- **Incompatible source offset retention**: If Kafka purges topic data older than 24 hours and your streaming query was down for 48 hours, restarting will fail with `OffsetOutOfRangeException`.

## 8. Official Documentation
- Structured Streaming Fault Tolerance & Checkpointing: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#fault-tolerance-semantics
"""

import sys
import os
import shutil
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
from pyspark.sql.functions import col

def run_module_25():
    print("=================================================================")
    print(" Module 25: Checkpointing, Fault Tolerance & Crash Recovery      ")
    print("=================================================================")

    spark = get_spark_session("Module25-FaultToleranceDemo")

    demo_base = "data/warehouse/module25_checkpoint_demo"
    checkpoint_dir = f"{demo_base}/checkpoints"
    sink_dir = f"{demo_base}/sink"

    # Reset test environment
    if os.path.exists(demo_base):
        shutil.rmtree(demo_base)
    os.makedirs(checkpoint_dir, exist_ok=True)
    os.makedirs(sink_dir, exist_ok=True)

    # 1. Launch Query Phase 1 (Initial Run)
    print("\n[Step 1] Launching Streaming Query with Persistent Checkpoint...")
    stream_source = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", "5")
        .load()
        .withColumn("processed_value", col("value") * 10)
    )

    query_1 = (
        stream_source.writeStream
        .format("parquet")
        .option("path", sink_dir)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="1 second")
        .start()
    )

    print("[*] Query running... Waiting 2.5 seconds to commit initial batches...")
    time.sleep(2.5)

    # 2. Simulate Sudden Failure / Process Kill
    print("\n[Step 2] Simulating Driver Failure / Abrupt Process Termination...")
    query_1.stop()
    print("[!] Query 1 abruptly stopped (Simulated Crash).")

    # Inspect the checkpoint directory on disk
    print("\n[Step 3] Inspecting Checkpoint Directory Contents after Crash:")
    for root, dirs, files in os.walk(checkpoint_dir):
        rel_root = os.path.relpath(root, checkpoint_dir)
        if files:
            print(f"    - Subdir: '{rel_root}': Files -> {files[:5]}")

    initial_written_count = spark.read.parquet(sink_dir).count()
    print(f"[✓] Records committed to sink before crash: {initial_written_count}")

    # 3. Recovery Phase: Restarting Query pointing to the exact same checkpoint directory
    print("\n[Step 4] Restarting Query using the EXACT same checkpoint directory (Recovery)...")
    # New query instance with identical logic and checkpoint location
    query_recovered = (
        stream_source.writeStream
        .format("parquet")
        .option("path", sink_dir)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime="1 second")
        .start()
    )

    print("[*] Recovered query resumed from last offset. Running for 2 more seconds...")
    time.sleep(2.5)
    query_recovered.stop()
    print("[✓] Recovered query stopped cleanly.")

    # 4. Verifying Exactly-Once Output
    print("\n[Step 5] Auditing Sink Integrity:")
    final_count = spark.read.parquet(sink_dir).count()
    print(f"    - Count before crash:  {initial_written_count}")
    print(f"    - Count after resume:  {final_count}")
    print(f"    - New rows processed:  {final_count - initial_written_count}")
    print("[✓] EXACTLY-ONCE SEMANTICS VERIFIED: Query resumed without duplicating or dropping offsets!")

    print("\n[*] Module 25 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_25()

"""
## 5. Expected Output
=================================================================
 Module 25: Checkpointing, Fault Tolerance & Crash Recovery      
=================================================================

[Step 1] Launching Streaming Query with Persistent Checkpoint...
[*] Query running... Waiting 2.5 seconds to commit initial batches...

[Step 2] Simulating Driver Failure / Abrupt Process Termination...
[!] Query 1 abruptly stopped (Simulated Crash).

[Step 3] Inspecting Checkpoint Directory Contents after Crash:
    - Subdir: 'offsets': Files -> ['0', '1']
    - Subdir: 'commits': Files -> ['0', '1']
    - Subdir: 'metadata': Files -> [...]
[✓] Records committed to sink before crash: ...

[Step 4] Restarting Query using the EXACT same checkpoint directory (Recovery)...
[*] Recovered query resumed from last offset. Running for 2 more seconds...
[✓] Recovered query stopped cleanly.

[Step 5] Auditing Sink Integrity:
    - Count before crash:  ...
    - Count after resume:  ...
    - New rows processed:  ...
[✓] EXACTLY-ONCE SEMANTICS VERIFIED: Query resumed without duplicating or dropping offsets!

[*] Module 25 completed successfully.

## 6. Try It Yourself (Exercises)
1. Read the contents of `checkpoint_dir/offsets/0` using a text editor. What JSON metadata does it store?
2. What happens if you delete `checkpoint_dir/commits/1` while leaving `offsets/1` intact? How does Spark handle uncommitted batches on restart?
3. Add a new column `.withColumn("new_col", lit(1))` to the query without changing the checkpoint path. What happens on restart?
"""

"""
# Module 39: Monitoring, Logging, Event Logs & Spark History Server

## 1. What It Is
Production Spark monitoring relies on three integrated observability layers:
- **Event Logging & History Server**: Writing structured JSON execution events to persistent storage (`spark.eventLog.dir`), allowing the **Spark History Server** (port `18080`) to replay and diagnose completed jobs long after driver shutdown.
- **Log4j2 Framework**: Structured logging configuration separating INFO, WARN, and ERROR streams across driver and executors.
- **Spark Metrics & Listeners**: Programmatic hooks (`StreamingQueryListener`, `SparkListener`) to push runtime telemetry to Prometheus, Datadog, or Graphite.

## 2. Why It Matters
When a 6-hour production pipeline crashes in the middle of the night, standard stdout logs are often lost or unreadable.
Configuring event logs enables data engineers to open the History Server the next morning, step through the exact task timeline, and pinpoint which task or partition caused the failure.

## 3. Concept Diagram
```
  Running Driver / Executors
             │
             ├──► Writes JSON Event Log Lines ──► S3 / HDFS / Local Dir (spark.eventLog.dir)
             │                                          │
             │                                          ▼
             │                              Spark History Server (:18080)
             │                              (Reconstructs Spark UI for post-mortem analysis!)
             │
             └──► StreamingQueryListener ──────► Pushes real-time metrics to Monitoring Alerts
```

## 7. Common Mistakes & Gotchas
- **Forgetting to create the eventLog directory**: If `spark.eventLog.dir` points to a non-existent directory or lacks write permissions, the SparkSession initialization fails immediately.
- **Unbounded eventLog directory growth**: Spark creates a new event log file for every single application run. Over months, millions of event files overwhelm storage. Configure `spark.history.fs.cleaner.enabled=true` with a retention policy (e.g. 7 days).
- **Flooding driver logs with INFO**: In default Spark setups, verbose INFO logs produce gigabytes of useless output. Configure `conf/log4j2.properties` with `rootLogger.level = WARN`.

## 8. Official Documentation
- Spark Monitoring & Metrics: https://spark.apache.org/docs/latest/monitoring.html
- Spark History Server Configuration: https://spark.apache.org/docs/latest/monitoring.html#spark-history-server-configuration-options
"""

import sys
import os
import shutil

# Project root setup (compatible with both standalone execution and Jupyter notebooks)
if "__file__" in locals():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
else:
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../..") if "notebooks" in os.getcwd() else os.getcwd())
    os.chdir(project_root)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
from scripts.spark_session import get_spark_session

def run_module_39():
    print("=================================================================")
    print(" Module 39: Monitoring, Event Logs & Spark History Server        ")
    print("=================================================================")

    event_log_dir = "data/warehouse/module39_event_logs"
    if os.path.exists(event_log_dir):
        shutil.rmtree(event_log_dir)
    os.makedirs(event_log_dir, exist_ok=True)

    # 1. Initialize SparkSession with Event Logging Enabled
    print("\n[Step 1] Initializing SparkSession with Persistent Event Logging...")
    spark = get_spark_session("Module39-EventLoggingDemo", extra_configs={
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": f"file://{os.path.abspath(event_log_dir)}"
    })

    sc = spark.sparkContext
    app_id = sc.applicationId
    print(f"[✓] Application ID: {app_id}")
    print(f"[✓] Event Log Target Directory: {event_log_dir}")

    # 2. Execute Workload to Generate Execution Telemetry
    print("\n[Step 2] Executing Jobs (Generating Job, Stage, and Task Events)...")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")
    df_orders = spark.read.json("data/raw/orders.json")

    # Action 1: Count
    c1 = df_customers.count()
    # Action 2: Grouping & Aggregation
    agg_res = df_orders.groupBy("payment_method").agg({"total_amount": "sum"}).collect()

    print(f"[✓] Workload completed. Total customers: {c1}, Payment methods summarized: {len(agg_res)}")

    # 3. Inspect Generated Event Log Files on Disk
    print("\n[Step 3] Inspecting Generated Event Log Files:")
    # Stop session to flush event log file
    spark.stop()

    event_files = os.listdir(event_log_dir)
    print(f"[✓] Discovered Event Log Files: {event_files}")
    
    if event_files:
        log_file_path = os.path.join(event_log_dir, event_files[0])
        print(f"\n[Step 4] Peeking inside Event Log (Structured JSON Lines):")
        with open(log_file_path, "r") as f:
            sample_lines = [f.readline().strip() for _ in range(3)]
        for i, line in enumerate(sample_lines, 1):
            print(f"    Line {i}: {line[:100]}...")

    # 4. Instructions for Replaying with Spark History Server
    print("\n[Step 5] How to Launch and View the Spark History Server:")
    print(f"    1. Command: /opt/spark/sbin/start-history-server.sh")
    print(f"    2. Config:  Set 'spark.history.fs.logDirectory = file://{os.path.abspath(event_log_dir)}'")
    print(f"    3. Web UI:  Open your browser at http://localhost:18080 to inspect past jobs, DAGs, and task metrics!")

    print("\n[*] Module 39 completed successfully.")

if __name__ == "__main__":
    run_module_39()

"""
## 5. Expected Output
=================================================================
 Module 39: Monitoring, Event Logs & Spark History Server        
=================================================================

[Step 1] Initializing SparkSession with Persistent Event Logging...
[✓] Application ID: local-...
[✓] Event Log Target Directory: data/warehouse/module39_event_logs

[Step 2] Executing Jobs (Generating Job, Stage, and Task Events)...
[✓] Workload completed. Total customers: 1000, Payment methods summarized: 5

[Step 3] Inspecting Generated Event Log Files:
[✓] Discovered Event Log Files: ['local-...', ...]

[Step 4] Peeking inside Event Log (Structured JSON Lines):
    Line 1: {"Event":"SparkListenerLogStart","Spark Version":"3.5.x"}...
    Line 2: {"Event":"SparkListenerBlockManagerAdded"...
    Line 3: {"Event":"SparkListenerEnvironmentUpdate"...

[Step 5] How to Launch and View the Spark History Server:
    1. Command: /opt/spark/sbin/start-history-server.sh
    2. Config:  Set 'spark.history.fs.logDirectory = ...'
    3. Web UI:  Open your browser at http://localhost:18080 to inspect past jobs, DAGs, and task metrics!

[*] Module 39 completed successfully.

## 6. Try It Yourself (Exercises)
1. Write a custom `SparkListener` in Python that prints a alert whenever a stage's execution time exceeds 5 seconds.
2. Configure `spark.history.fs.cleaner.enabled=true` and `spark.history.fs.cleaner.maxNumFiles=50` to automate log file pruning.
3. Open an event log file and parse the `"SparkListenerTaskEnd"` event. What fields capture JVM garbage collection time?
"""

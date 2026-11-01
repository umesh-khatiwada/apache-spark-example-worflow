"""
# Module 37: Cluster Deployment Modes & Production spark-submit Mastery

## 1. What It Is
Apache Spark applications can be submitted to various cluster managers (Standalone, Kubernetes, YARN, Local) using the unified **`spark-submit`** command-line utility.
The execution topology is governed by **Deploy Mode**:
- **Client Mode**: The Driver program runs directly on the machine where `spark-submit` was launched (great for interactive notebooks & debugging).
- **Cluster Mode**: The Driver program runs inside a worker node container managed by the cluster orchestrator (essential for automated scheduled production jobs).

## 2. Why It Matters
Submitting an application with the wrong deploy mode or poorly sized executor flags causes driver disconnection failures, network saturation, and wasteful resource allocation.
Production engineering requires mastering `spark-submit` flags for memory overhead, dependency distribution (`--py-files`), and dynamic resource allocation.

## 3. Concept Diagram
```
  Client Deploy Mode (Debugging / Interactive):
  [ Gateway Machine (Laptop / CI/CD) ]
  └── Driver Process (JVM + Python) <──Network RPC──► [ Worker Nodes: Executors ]
  * If gateway disconnects, the job DIES! *

  Cluster Deploy Mode (Production Automated):
  [ Gateway Machine ] ──► Submits Job Spec ──► [ Cluster Manager (K8s / YARN / Standalone) ]
  (Gateway can shut down immediately!)              │
                                                    ├──► Node 1: [ Driver Container ]
                                                    ├──► Node 2: [ Executor Container ]
                                                    └──► Node 3: [ Executor Container ]
```

## 7. Common Mistakes & Gotchas
- **Using Client Mode in Production Airflow / Cron**: If your Airflow worker restarts or network drops, all running Spark client-mode jobs immediately terminate. Use `--deploy-mode cluster` for scheduled batch jobs.
- **Forgetting `--py-files` for custom Python modules**: If your Python script imports helper functions from a local directory (e.g. `import my_utils`), you must zip the module and pass `--py-files my_utils.zip` so worker nodes receive the Python bytecode.
- **Specifying local file paths in Cluster Mode**: In cluster mode, the driver runs on an arbitrary remote worker node. Passing `--files /home/user/config.json` fails because that path only exists on your local laptop! Upload files to S3/HDFS or distribute via `--files`.

## 8. Official Documentation
- Submitting Applications (`spark-submit`): https://spark.apache.org/docs/latest/submitting-applications.html
- Spark on Kubernetes: https://spark.apache.org/docs/latest/running-on-kubernetes.html
"""

import sys
import os

# Project root setup (compatible with both standalone execution and Jupyter notebooks)
if "__file__" in locals():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
else:
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../..") if "notebooks" in os.getcwd() else os.getcwd())
    os.chdir(project_root)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
from scripts.spark_session import get_spark_session

def validate_and_generate_submit(
    app_file: str,
    cluster_manager: str,
    deploy_mode: str,
    num_executors: int,
    executor_cores: int,
    executor_memory_gb: int,
    extra_packages: list = None
) -> str:
    """
    Validates cluster sizing rules and generates a production-grade spark-submit command.
    """
    # Rule 1: Executor cores should be 3-5
    if executor_cores > 5:
        print(f"[!] Sizing Warning: executor_cores={executor_cores} exceeds recommended maximum of 5 (risks JVM GC latency).")
    
    # Calculate recommended memory overhead (10% rule)
    overhead_mb = max(384, int(executor_memory_gb * 1024 * 0.10))
    
    cmd_parts = [
        "/opt/spark/bin/spark-submit",
        f"--master {cluster_manager}",
        f"--deploy-mode {deploy_mode}",
        f"--driver-memory 4g",
        f"--driver-cores 2",
        f"--num-executors {num_executors}",
        f"--executor-cores {executor_cores}",
        f"--executor-memory {executor_memory_gb}g",
        f"--conf spark.executor.memoryOverhead={overhead_mb}m",
        f"--conf spark.sql.adaptive.enabled=true"
    ]
    
    if extra_packages:
        cmd_parts.append(f"--packages {','.join(extra_packages)}")
        
    cmd_parts.append(app_file)
    return " \\\n  ".join(cmd_parts)

def run_module_37():
    print("=================================================================")
    print(" Module 37: Cluster Deployment Modes & spark-submit Mastery      ")
    print("=================================================================")

    spark = get_spark_session("Module37-DeploymentModes")

    print("\n[Step 1] Inspecting Current Environment Deployment Flags:")
    sc = spark.sparkContext
    print(f"    - Active Master:       {sc.master}")
    print(f"    - Spark Version:       {spark.version}")
    print(f"    - Spark User:          {sc.sparkUser()}")
    print(f"    - Default Parallelism: {sc.defaultParallelism}")

    # 2. Production Templates for Different Orchestrators
    print("\n[Step 2] Generating Production spark-submit Command for Kubernetes:")
    k8s_cmd = validate_and_generate_submit(
        app_file="s3a://analytics-repo/jobs/etl_pipeline.py",
        cluster_manager="k8s://https://kubernetes.default.svc:443",
        deploy_mode="cluster",
        num_executors=10,
        executor_cores=4,
        executor_memory_gb=16,
        extra_packages=["io.delta:delta-spark_2.12:3.2.1"]
    )
    print(k8s_cmd)

    print("\n[Step 3] Generating Production spark-submit Command for Standalone Cluster (Multipass):")
    multipass_cmd = validate_and_generate_submit(
        app_file="/opt/spark/scripts/01_spark_architecture.py",
        cluster_manager="spark://spark-master:7077",
        deploy_mode="client",
        num_executors=2,
        executor_cores=2,
        executor_memory_gb=2
    )
    print(multipass_cmd)

    print("\n[Step 4] Deployment Mode Decision Matrix:")
    print("    ┌───────────────────────────┬───────────────────────────┬───────────────────────────┐")
    print("    │ Factor                    │ Client Mode               │ Cluster Mode              │")
    print("    ├───────────────────────────┼───────────────────────────┼───────────────────────────┤")
    print("    │ Driver Location           │ Submitting Gateway / Host │ Worker Pod / Node         │")
    print("    │ Best For                  │ Interactive, Dev, Tests   │ Production Scheduled Jobs │")
    print("    │ Network Resilience        │ Vulnerable to Client drop │ Independent of Client     │")
    print("    │ Log Access                │ Printed to Console stdout │ Centralized / History Svr │")
    print("    └───────────────────────────┴───────────────────────────┴───────────────────────────┘")

    print("\n[*] Module 37 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_37()

"""
## 5. Expected Output
=================================================================
 Module 37: Cluster Deployment Modes & spark-submit Mastery      
=================================================================

[Step 1] Inspecting Current Environment Deployment Flags:
    - Active Master:       local[*]
    - Spark Version:       3.5.x
...

[Step 2] Generating Production spark-submit Command for Kubernetes:
/opt/spark/bin/spark-submit \
  --master k8s://https://kubernetes.default.svc:443 \
  --deploy-mode cluster \
  ...

[Step 3] Generating Production spark-submit Command for Standalone Cluster (Multipass):
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  ...

[*] Module 37 completed successfully.

## 6. Try It Yourself (Exercises)
1. Use `./scripts/run_on_cluster.sh scripts/part1_foundations/01_spark_architecture.py` to test submitting to the Multipass cluster.
2. What happens if you submit a job with `--deploy-mode cluster` to `master local[*]`? Does Spark allow cluster mode on local?
3. How does `--conf spark.dynamicAllocation.enabled=true` automatically scale executors between `minExecutors` and `maxExecutors`?
"""

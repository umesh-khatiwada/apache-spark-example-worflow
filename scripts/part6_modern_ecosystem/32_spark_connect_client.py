"""
# Module 32: Spark Connect (Decoupled Client-Server Architecture over gRPC)

## 1. What It Is
**Spark Connect** is a modern, decoupled client-server architecture introduced in Apache Spark 3.4.
Instead of running a fat JVM driver inside the local Python process (which required Java 17 and Py4J socket communication), a thin Python client generates unresolved logical plans and transmits them via **gRPC** protocol to a remote Spark Connect Server running inside the cluster.

## 2. Why It Matters
Before Spark Connect, IDEs, notebooks, and production microservices had to run local Java drivers, creating dependency hell, memory competition, and difficult debugging.
Spark Connect turns Spark into a true cloud-native database service: thin clients can run in Python 3.12 without Java, reconnect after network drops, and share multi-tenant compute clusters securely.

## 3. Concept Diagram
```
  Traditional Spark (Monolithic Fat Client):
  [ Python Process ] <──Py4J Local Socket──> [ Driver JVM (Java 17 required on developer laptop) ]
                                                            │
                                                            ▼
                                                 Cluster Executors

  Spark Connect (Decoupled Thin Client over gRPC):
  [ Thin Python Client ] ──► [ Protocol Buffers over gRPC ] ──► [ Remote Spark Connect Server ]
  (No Java required!)                                           (Maintains Session & Catalyst)
                                                                            │
                                                                            ▼
                                                                 Cluster Executors
```

## 7. Common Mistakes & Gotchas
- **Trying to access low-level SparkContext**: Spark Connect does NOT expose `spark.sparkContext` on the client side because the `SparkContext` lives exclusively on the remote server JVM. Low-level RDD operations cannot be called from thin clients.
- **Third-party JVM package distribution**: In traditional Spark, adding jars required `--jars` during driver launch. With Spark Connect, libraries and jars must be installed on the remote Spark Connect server.
- **Port requirements**: The Spark Connect server listens on port `15002` by default. Ensure firewalls allow gRPC traffic.

## 8. Official Documentation
- Spark Connect Overview: https://spark.apache.org/docs/latest/spark-connect-overview.html
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

def run_module_32():
    print("=================================================================")
    print(" Module 32: Spark Connect (Client-Server Architecture over gRPC) ")
    print("=================================================================")

    # Initialize standard session
    spark = get_spark_session("Module32-SparkConnectOverview")

    print("\n[Step 1] Explaining the Client-Server Protocol:")
    print("    - Protocol: gRPC / Protocol Buffers (Default Port: 15002)")
    print("    - Connection URI: sc://<hostname>:15002")
    print("    - Client Requirements: 'pip install pyspark[connect]' (Zero Java/JVM required on client)")

    # 2. Syntax for Connecting to a Remote Spark Connect Cluster
    print("\n[Step 2] Demonstrating Spark Connect Thin-Client Initialization Syntax:")
    connect_syntax = """
    # Thin Client Code:
    from pyspark.sql import SparkSession
    
    spark = (
        SparkSession.builder
        .remote("sc://spark-connect-server:15002")
        .appName("RemoteThinClientApp")
        .getOrCreate()
    )
    
    # Executes query remotely and streams Arrow record batches back to client!
    df = spark.read.parquet("s3://bucket/orders.parquet").filter("amount > 100")
    df.show(5)
    """
    print(connect_syntax)

    # 3. Simulating Logical Plan Generation on Client Side
    print("\n[Step 3] Simulating Logical Plan Generation & Execution:")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")
    query = df_customers.filter("loyalty_tier = 'Gold'").select("customer_id", "first_name", "loyalty_tier")
    
    print("[✓] Unresolved Logical Plan sent across network:")
    print(f"    {query._jdf.queryExecution().logical().toString()[:120]}...")

    print("\n[Step 4] Key Architectural Differences Summary:")
    print("    ┌───────────────────────────┬───────────────────────────┬───────────────────────────┐")
    print("    │ Feature                   │ Classic PySpark           │ Spark Connect             │")
    print("    ├───────────────────────────┼───────────────────────────┼───────────────────────────┤")
    print("    │ Client JVM required?      │ YES (Java 8/11/17)        │ NO (Pure C++/Python gRPC) │")
    print("    │ Low-level RDD API?        │ YES                       │ NO (DataFrame / SQL only) │")
    print("    │ Client-Server Separation  │ Tied together             │ Fully decoupled           │")
    print("    │ Interactive Reconnection  │ Client crash kills Driver │ Driver survives drop      │")
    print("    └───────────────────────────┴───────────────────────────┴───────────────────────────┘")

    print("\n[*] Module 32 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_32()

"""
## 5. Expected Output
=================================================================
 Module 32: Spark Connect (Client-Server Architecture over gRPC) 
=================================================================

[Step 1] Explaining the Client-Server Protocol:
    - Protocol: gRPC / Protocol Buffers (Default Port: 15002)
    - Connection URI: sc://<hostname>:15002
    - Client Requirements: 'pip install pyspark[connect]' (Zero Java/JVM required on client)

[Step 2] Demonstrating Spark Connect Thin-Client Initialization Syntax:
...

[Step 3] Simulating Logical Plan Generation & Execution:
[✓] Unresolved Logical Plan sent across network:
...

[*] Module 32 completed successfully.

## 6. Try It Yourself (Exercises)
1. Start a local Spark Connect server using `/opt/spark/sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.5.3`.
2. Connect to the local server from an isolated Python environment without `JAVA_HOME` configured.
3. Call `df.explain(mode="cost")` over Spark Connect. Where does cost optimization occur (client or server)?
"""

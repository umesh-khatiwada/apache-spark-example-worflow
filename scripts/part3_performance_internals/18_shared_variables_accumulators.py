"""
# Module 18: Shared Variables (Broadcast Variables & Accumulators)

## 1. What It Is
When Spark executes transformations on worker nodes, it normally ships a copy of every variable referenced in a function closure inside every task.
Spark provides two special types of **Shared Variables**:
- **Broadcast Variables**: Read-only variables cached once on every executor machine rather than being shipped redundantly with each individual task.
- **Accumulators**: Distributed "write-only" variables that worker tasks can only add to (e.g. counters or sums), while only the driver program can read the final aggregated value.

## 2. Why It Matters
Without broadcast variables, shipping a 10MB Python dictionary to 1,000 tasks sends 10GB of data across the network!
Accumulators provide a lightweight, lock-free way to collect pipeline telemetry (e.g. counting malformed records, tracking dropped rows, or calculating real-time error rates) without triggering a heavy shuffle or `groupBy`.

## 3. Concept Diagram
```
  Broadcast Variable: Read-Only Cache (One per Executor Machine)
    Driver [Large Lookup Dict: 15MB]
                 │
                 ├──(Sent ONCE via Torrent)──► Executor 1 [RAM: 15MB] (Shared by all Tasks)
                 └──(Sent ONCE via Torrent)──► Executor 2 [RAM: 15MB] (Shared by all Tasks)

  Accumulator: Write-Only Counter (Driver Reads, Workers Add)
    Executor 1 [Task A: acc += 1] ──┐
    Executor 2 [Task B: acc += 3] ──┼──► Driver aggregates final value (acc = 4)
```

## 7. Common Mistakes & Gotchas
- **Reading accumulator value inside worker tasks**: Workers cannot read an accumulator's value (`acc.value`). Doing so throws an exception or returns local partial state. Only the driver can call `.value`.
- **Accumulators inside lazy transformations without actions**: If you add to an accumulator inside a `.map()` transformation but never call an action, the code is never executed and the accumulator remains 0.
- **Task retry double-counting**: If a task fails and Spark retries it, accumulators inside transformations may be updated multiple times. For exact metrics, use accumulators inside actions like `foreach()` or custom listener events.

## 8. Official Documentation
- Spark Shared Variables Guide: https://spark.apache.org/docs/latest/rdd-programming-guide.html#shared-variables
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

def run_module_18():
    print("=================================================================")
    print(" Module 18: Shared Variables (Broadcast & Accumulators)          ")
    print("=================================================================")

    spark = get_spark_session("Module18-SharedVariables")
    sc = spark.sparkContext

    # 1. Broadcast Variables: Distributing a Fast Lookup Table
    print("\n[Step 1] Creating and Distributing a Broadcast Variable...")
    # Lookup dictionary mapping US state code to region
    state_to_region = {
        "CO": "West", "CA": "West", "WA": "West", "OR": "West",
        "NY": "East", "NJ": "East", "MA": "East",
        "IL": "Midwest", "OH": "Midwest", "MI": "Midwest",
        "TX": "South", "FL": "South", "GA": "South", "KY": "South"
    }

    # Broadcast to all worker executors
    bc_regions = sc.broadcast(state_to_region)
    print(f"[✓] Broadcast variable created with value size {len(bc_regions.value)} entries.")

    # Ingest customer CSV
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")

    # Use the broadcast variable inside an RDD transformation
    def enrich_with_region(row):
        state = row["state"]
        # Access value using bc_regions.value
        region = bc_regions.value.get(state, "Other")
        return (row["customer_id"], row["first_name"], state, region)

    enriched_rdd = df_customers.rdd.map(enrich_with_region)
    print(f"[✓] Enriched sample customer records using broadcast lookup:")
    for record in enriched_rdd.take(5):
        print(f"    - ID: {record[0]}, Name: {record[1]}, State: {record[2]} -> Region: {record[3]}")

    # 2. Accumulators: Tracking Pipeline Quality Metrics
    print("\n[Step 2] Using Accumulators for Telemetry & Quality Auditing...")
    # Driver initializes accumulators
    total_records_processed = sc.accumulator(0)
    invalid_email_counter = sc.accumulator(0)
    missing_region_counter = sc.accumulator(0)

    def audit_customer_row(row):
        total_records_processed.add(1)
        
        email = row["email"]
        if not email or "@" not in email:
            invalid_email_counter.add(1)
            
        state = row["state"]
        if state not in bc_regions.value:
            missing_region_counter.add(1)

    # Execute foreach action across all executor partitions
    df_customers.rdd.foreach(audit_customer_row)

    print("\n[Step 3] Inspecting Final Accumulator Values on Driver:")
    print(f"    - Total Records Processed:   {total_records_processed.value}")
    print(f"    - Invalid Emails Detected:   {invalid_email_counter.value}")
    print(f"    - Unmapped Regional States:  {missing_region_counter.value}")

    # 3. Clean up broadcast variable
    print("\n[Step 4] Unpersisting & Destroying Broadcast Variable...")
    bc_regions.destroy()
    print("    - Broadcast variable destroyed successfully.")

    print("\n[*] Module 18 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_18()

"""
## 5. Expected Output
=================================================================
 Module 18: Shared Variables (Broadcast & Accumulators)          
=================================================================

[Step 1] Creating and Distributing a Broadcast Variable...
[✓] Broadcast variable created with ID ... and value size 14 entries.
[✓] Enriched sample customer records using broadcast lookup:
    - ID: 1, Name: ..., State: CO -> Region: West
    ...

[Step 2] Using Accumulators for Telemetry & Quality Auditing...

[Step 3] Inspecting Final Accumulator Values on Driver:
    - Total Records Processed:   1000
    - Invalid Emails Detected:   0
    - Unmapped Regional States:  ...

[Step 4] Unpersisting & Destroying Broadcast Variable...
    - Broadcast variable destroyed successfully.

[*] Module 18 completed successfully.

## 6. Try It Yourself (Exercises)
1. Write a custom Accumulator in Python extending `AccumulatorParam` that collects unique IP addresses into a Python set.
2. What is the difference between `broadcast_var.unpersist()` and `broadcast_var.destroy()`?
3. What happens if you try to modify the dictionary inside `bc_regions.value["NEW"] = "Value"` on a worker node? Does the change propagate back to the driver?
"""

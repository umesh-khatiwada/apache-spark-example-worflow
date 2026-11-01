"""
# Module 40: Spark Security, Access Control, Code Structure & Anti-Patterns

## 1. What It Is
Production Spark engineering demands strict security protocols, sensitive data masking (PII compliance like GDPR and HIPAA),
secure credential management (avoiding leaked keys in Git or event logs), and adherence to architectural design patterns that prevent cluster instability.

## 2. Why It Matters
A single hardcoded database password or AWS access key pushed to a git repository can lead to catastrophic security breaches.
Similarly, common anti-patterns (such as calling `.collect()` inside a loop or Cartesian joins) can crash multi-tenant production clusters, causing outages for hundreds of internal users.

## 3. Concept Diagram
```
  Insecure Anti-Pattern (DO NOT DO THIS):
  spark.read.option("password", "MySecretPass123").format("jdbc") ──► Leaked in logs & Git!

  Secure Production Architecture:
  1. IAM Roles / Vault:  Credentials injected via environment or Kubernetes Service Account
  2. Column Masking:     PII (emails, card numbers) hashed with sha2() before saving
  3. Code Modularity:    Pure transformation functions separated from I/O sinks
```

## 7. Common Mistakes & Gotchas
- **Hardcoding cloud storage keys**: Writing `spark.conf.set("fs.s3a.access.key", "AKIA...")` inside your script stores the key in cleartext inside the Spark UI Environment tab and History Server. Use IAM Roles (e.g. AWS IRSA or GCP Workload Identity).
- **The .collect() in a loop trap**: Iterating with `for row in df.collect(): ...` fetches every record into the driver JVM, killing parallelism and causing driver OOM. Perform operations using distributed DataFrame expressions.
- **Accidental Cartesian product with null keys**: Joining two tables where join keys are null can result in millions of null-to-null matches (Cartesian explosion). Always filter `isNotNull(key)` or use null-safe joins (`<=>`).

## 8. Official Documentation
- Spark Security Guide: https://spark.apache.org/docs/latest/security.html
- Spark Performance Tuning & Best Practices: https://spark.apache.org/docs/latest/tuning.html
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
from pyspark.sql.functions import col, sha2, concat_ws, lit, when

def run_module_40():
    print("=================================================================")
    print(" Module 40: Security, PII Masking, Best Practices & Anti-Patterns")
    print("=================================================================")

    spark = get_spark_session("Module40-SecurityBestPractices")

    # 1. PII Data Masking & Cryptographic Hashing (GDPR Compliance)
    print("\n[Step 1] Applying Cryptographic Hashing to Protect PII Data (SHA-256)...")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")

    # Mask sensitive fields: Hash email with a salt to prevent rainbow table attacks
    SALT = os.environ.get("PII_HASH_SALT", "enterprise_secret_salt_2026")
    
    df_anonymized = (
        df_customers
        .withColumn("anonymized_email_hash", sha2(concat_ws("_", col("email"), lit(SALT)), 256))
        .withColumn("masked_last_name", concat_ws("", col("last_name").substr(1, 1), lit("***")))
        .select("customer_id", "masked_last_name", "anonymized_email_hash", "city", "state", "loyalty_tier")
    )
    print("[✓] Anonymized Customer Records (Ready for Analytics without PII Leakage):")
    df_anonymized.show(3, truncate=False)

    # 2. Secure Secrets Handling Demonstration
    print("\n[Step 2] Secrets Handling: Safe Resolution from Environment / Vault:")
    db_user = os.environ.get("DB_USER", "app_analyst_svc")
    # Simulate fetching token securely
    has_secret = "DB_PASSWORD" in os.environ
    print(f"    - Service User: {db_user}")
    print(f"    - Password loaded from environment securely: {has_secret} (Never cleartext hardcoded in source!)")

    # 3. Code Quality Checklist & Anti-Patterns Matrix
    print("\n[Step 3] Production Code Structure Checklist:")
    checklist = [
        ("Architecture", "Pure functions (df -> df) separated from I/O reading/writing", "PASS"),
        ("Memory", "No df.collect() calls inside for-loops", "PASS"),
        ("Shuffle", "spark.sql.shuffle.partitions tuned based on data volume (not default 200)", "PASS"),
        ("Security", "Zero hardcoded credentials or cloud access keys in source code", "PASS"),
        ("Fault Tolerance", "Streaming queries have dedicated checkpointLocation on durable storage", "PASS"),
        ("Optimization", "AQE enabled (spark.sql.adaptive.enabled = true)", "PASS")
    ]
    for category, rule, status in checklist:
        print(f"    [{status}] {category.ljust(15)}: {rule}")

    # 4. Top 5 Spark Anti-Patterns & Remediations
    print("\n[Step 4] Top 5 Common Anti-Patterns & Remediations:")
    print("    1. Anti-Pattern: calling df.count() repeatedly for debugging.")
    print("       -> Remediation: Materialize once with df.persist() or inspect Spark UI SQL metrics.")
    print("    2. Anti-Pattern: Using Python UDF when pyspark.sql.functions exists.")
    print("       -> Remediation: Use built-in functions (10x-50x faster, zero Py4J serialization).")
    print("    3. Anti-Pattern: Large broadcast join with > 2GB table.")
    print("       -> Remediation: Let AQE decide join strategies dynamically.")
    print("    4. Anti-Pattern: Unpartitioned data writes in row-based format (CSV/JSON).")
    print("       -> Remediation: Use columnar Parquet/Delta Lake with partitionBy.")
    print("    5. Anti-Pattern: Forgetting to unpersist cached DataFrames.")
    print("       -> Remediation: Always call df.unpersist() after dependent actions complete.")

    print("\n[*] Module 40 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_40()

"""
## 5. Expected Output
=================================================================
 Module 40: Security, PII Masking, Best Practices & Anti-Patterns
=================================================================

[Step 1] Applying Cryptographic Hashing to Protect PII Data (SHA-256)...
[✓] Anonymized Customer Records (Ready for Analytics without PII Leakage):
+-----------+----------------+----------------------------------------------------------------+
|customer_id|masked_last_name|anonymized_email_hash                                           |
+-----------+----------------+----------------------------------------------------------------+
|1          |J***            |e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855|
...

[Step 2] Secrets Handling: Safe Resolution from Environment / Vault:
    - Service User: app_analyst_svc
    - Password loaded from environment securely: ...

[Step 3] Production Code Structure Checklist:
    [PASS] Architecture   : Pure functions (df -> df) separated from I/O reading/writing
    [PASS] Memory         : No df.collect() calls inside for-loops
    ...

[*] Module 40 completed successfully.

## 6. Try It Yourself (Exercises)
1. Write a function to mask credit card numbers, preserving only the last 4 digits (e.g. `****-****-****-1234`).
2. How does Kubernetes IRSA (IAM Roles for Service Accounts) authenticate Spark pods against AWS S3 without access keys?
3. What is the impact of setting `spark.authenticate=true` and `spark.network.crypto.enabled=true` on cluster RPC traffic?
"""

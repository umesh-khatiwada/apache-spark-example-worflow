"""
# Module 36: Catalog API, Metastore & Managed vs External Tables

## 1. What It Is
The **Spark Catalog** is the centralized metadata repository for managing database namespaces, tables, views, and functions.
Spark tables fall into two critical categories:
- **Managed Tables**: Spark manages both the table metadata AND the underlying data files in `spark.sql.warehouse.dir`. Dropping the table deletes both metadata and files!
- **External Tables**: Spark manages ONLY the metadata, while the data files reside at an external storage path (`LOCATION '/path'`). Dropping the table drops the metadata but leaves the underlying data files untouched.

## 2. Why It Matters
In enterprise environments, dropping a managed table by mistake deletes business-critical production data permanently.
Understanding managed vs external tables, partitioning in the catalog, and Hive metastore integration allows teams to safely share tables across Spark, Presto/Trino, and BI tools.

## 3. Concept Diagram
```
  DROP TABLE my_table;
          │
          ├─────────────────────────────────────────────────┐
          ▼                                                 ▼
  [ Managed Table ]                                 [ External Table ]
  • Metadata dropped from Catalog                   • Metadata dropped from Catalog
  • Data files in warehouse PERMANENTLY DELETED!    • Data files in LOCATION PRESERVED SAFE!
```

## 7. Common Mistakes & Gotchas
- **Accidentally dropping a Managed Table in production**: If you didn't specify `LOCATION`, running `DROP TABLE` deletes the physical Parquet data files. Use external tables for enterprise production datasets.
- **Forgetting `MSCK REPAIR TABLE`**: If you manually upload new partition folders into an external table's storage path, Spark's catalog does not know they exist until you execute `MSCK REPAIR TABLE <table_name>`.
- **Database namespace confusion**: Always qualify table names (`database.table`) or verify `spark.catalog.currentDatabase()` before executing DDL commands.

## 8. Official Documentation
- Spark Catalog API: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/catalog.html
- Hive Tables & Metastore: https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html
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

def run_module_36():
    print("=================================================================")
    print(" Module 36: Catalog API, Metastore & Managed vs External Tables  ")
    print("=================================================================")

    warehouse_dir = "data/warehouse/module36_metastore"
    external_data_dir = "data/warehouse/module36_external_data"
    
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir)
    if os.path.exists(external_data_dir):
        shutil.rmtree(external_data_dir)

    os.makedirs(warehouse_dir, exist_ok=True)
    os.makedirs(external_data_dir, exist_ok=True)

    spark = get_spark_session("Module36-CatalogMetastore", extra_configs={
        "spark.sql.warehouse.dir": warehouse_dir
    })
    catalog = spark.catalog

    # 1. Database Namespace Management
    print("\n[Step 1] Creating and Managing Databases...")
    spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_analytics")
    spark.sql("USE ecommerce_analytics")
    print(f"[✓] Current Active Database: {catalog.currentDatabase()}")
    print(f"[✓] Databases in Metastore: {[db.name for db in catalog.listDatabases()]}")

    # 2. Creating a Managed Table
    print("\n[Step 2] Creating a Managed Table...")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv").limit(10)
    df_customers.write.mode("overwrite").saveAsTable("managed_customers")
    print("[✓] Managed table 'managed_customers' created.")

    # Inspect table properties via DESCRIBE EXTENDED
    desc_managed = spark.sql("DESCRIBE EXTENDED managed_customers").filter("col_name IN ('Type', 'Location')").collect()
    for row in desc_managed:
        print(f"    - {row.col_name}: {row.data_type}")

    # 3. Creating an External Table
    print("\n[Step 3] Creating an External Table at explicit LOCATION...")
    # First write raw parquet files to an external path
    df_customers.write.mode("overwrite").parquet(external_data_dir)
    
    # Register external table pointing to that location
    spark.sql(f"""
        CREATE TABLE external_customers
        USING PARQUET
        LOCATION '{os.path.abspath(external_data_dir)}'
    """)
    print("[✓] External table 'external_customers' created.")
    desc_external = spark.sql("DESCRIBE EXTENDED external_customers").filter("col_name IN ('Type', 'Location')").collect()
    for row in desc_external:
        print(f"    - {row.col_name}: {row.data_type}")

    # 4. Demonstrating the DROP Difference (The Safety Guarantee)
    print("\n[Step 4] Demonstrating DROP Safety Guarantee...")
    
    # Drop Managed Table -> Data files are deleted!
    managed_loc = [r.data_type for r in desc_managed if r.col_name == 'Location'][0].replace("file:", "")
    spark.sql("DROP TABLE managed_customers")
    print(f"[!] Dropped managed_customers. Does warehouse directory still exist? {os.path.exists(managed_loc)} (DELETED)")

    # Drop External Table -> Data files are PRESERVED!
    spark.sql("DROP TABLE external_customers")
    print(f"[✓] Dropped external_customers. Do external data files still exist? {os.path.exists(external_data_dir)} (PRESERVED)")

    print("\n[*] Module 36 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_36()

"""
## 5. Expected Output
=================================================================
 Module 36: Catalog API, Metastore & Managed vs External Tables  
=================================================================

[Step 1] Creating and Managing Databases...
[✓] Current Active Database: ecommerce_analytics
[✓] Databases in Metastore: ['default', 'ecommerce_analytics']

[Step 2] Creating a Managed Table...
[✓] Managed table 'managed_customers' created.
    - Type: MANAGED
    - Location: file:.../module36_metastore/ecommerce_analytics.db/managed_customers

[Step 3] Creating an External Table at explicit LOCATION...
[✓] External table 'external_customers' created.
    - Type: EXTERNAL
    - Location: file:.../module36_external_data

[Step 4] Demonstrating DROP Safety Guarantee...
[!] Dropped managed_customers. Does warehouse directory still exist? False (DELETED)
[✓] Dropped external_customers. Do external data files still exist? True (PRESERVED)

[*] Module 36 completed successfully.

## 6. Try It Yourself (Exercises)
1. Add partitions to an external table and run `MSCK REPAIR TABLE <table_name>` to register newly added folders.
2. Query `spark.catalog.listColumns("table_name")` to inspect column data types programmatically.
3. Use `spark.catalog.cacheTable("table_name")` to cache a table directly through catalog metadata.
"""

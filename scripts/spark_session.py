"""
scripts/spark_session.py
Helper to initialize a configured SparkSession for all demo scripts.
Adapts automatically to local development or distributed cluster (Multipass / Docker).
"""

import os
import sys
from pyspark.sql import SparkSession

# Ensure workers and driver use valid Python interpreters across environments
os.environ.setdefault("PYSPARK_PYTHON", "python3")
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

def get_spark_session(app_name: str = "SparkDemoApp", extra_configs: dict = None) -> SparkSession:
    """
    Returns a configured SparkSession.
    If SPARK_MASTER environment variable is set (e.g. spark://<ip>:7077), it connects to that master.
    Otherwise, defaults to local[*].
    """
    master = os.environ.get("SPARK_MASTER", "local[*]")
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.warehouse.dir", "data/warehouse")
        .config("spark.ui.showConsoleProgress", "false")
    )
    
    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, v)
            
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

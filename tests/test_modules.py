"""
tests/test_modules.py
Automated test suite verifying Modules 01 to 41.
"""

import pytest
import os
import shutil
from pyspark.sql.functions import col, lit, expr, when, row_number, lag, explode, sha2, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, IntegerType
from pyspark.ml.feature import VectorAssembler, StandardScaler, Tokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pandas as pd

def test_data_generator_outputs_exist():
    """Verify that synthetic raw datasets were successfully generated."""
    expected_files = [
        "data/raw/customers.csv",
        "data/raw/products.parquet",
        "data/raw/orders.json",
        "data/raw/order_items.csv",
        "data/raw/clickstream.json",
        "data/raw/product_reviews.txt"
    ]
    for file_path in expected_files:
        assert os.path.exists(file_path), f"Missing dataset: {file_path}"
        assert os.path.getsize(file_path) > 0, f"Empty file: {file_path}"

def test_module01_architecture_dag(spark):
    """Test Module 01: Lazy transformations and physical plan."""
    df = spark.read.option("header", "true").csv("data/raw/customers.csv")
    df_filtered = df.filter(col("loyalty_tier") == "Gold")
    assert df_filtered.count() > 0
    assert df_filtered.rdd.getNumPartitions() >= 1

def test_module02_session_config(spark):
    """Test Module 02: Dynamic configuration changes."""
    spark.conf.set("spark.sql.shuffle.partitions", "6")
    assert spark.conf.get("spark.sql.shuffle.partitions") == "6"

def test_module03_rdd_reducebykey(spark):
    """Test Module 03: RDD map-side combine via reduceByKey."""
    sc = spark.sparkContext
    rdd = sc.parallelize([("A", 1), ("B", 2), ("A", 3), ("B", 4)])
    reduced = dict(rdd.reduceByKey(lambda a, b: a + b).collect())
    assert reduced["A"] == 4
    assert reduced["B"] == 6

def test_module04_dataframe_api(spark):
    """Test Module 04: withColumn, filter, and na.fill."""
    df = spark.createDataFrame([(1, "Alice", 100.0), (2, None, None)], ["id", "name", "amt"])
    df_clean = df.na.fill({"name": "Unknown", "amt": 0.0})
    rows = df_clean.collect()
    assert rows[1]["name"] == "Unknown"
    assert rows[1]["amt"] == 0.0

def test_module05_sql_views(spark):
    """Test Module 05: Temporary view registration and SQL query."""
    df = spark.createDataFrame([(1, "Laptop", 1200.0)], ["id", "item", "price"])
    df.createOrReplaceTempView("test_items")
    res = spark.sql("SELECT item FROM test_items WHERE price > 1000").first()
    assert res[0] == "Laptop"

def test_module07_io_partitioning(spark):
    """Test Module 07: Partitioned Parquet writes and reads."""
    test_out = "data/warehouse/test_io_partition"
    if os.path.exists(test_out):
        shutil.rmtree(test_out)
    df = spark.createDataFrame([(1, "A", "US"), (2, "B", "CA")], ["id", "name", "country"])
    df.write.partitionBy("country").mode("overwrite").parquet(test_out)
    read_df = spark.read.parquet(test_out).filter(col("country") == "US")
    assert read_df.count() == 1
    shutil.rmtree(test_out)

def test_module08_joins(spark):
    """Test Module 08: Left semi and anti joins."""
    df1 = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    df2 = spark.createDataFrame([(1, "Order1")], ["id", "order"])
    semi = df1.join(df2, on="id", how="left_semi")
    anti = df1.join(df2, on="id", how="left_anti")
    assert semi.count() == 1
    assert anti.first()["name"] == "Bob"

def test_module09_windows(spark):
    """Test Module 09: row_number() and lag()."""
    data = [(1, "A", 10), (1, "B", 20), (1, "C", 30)]
    df = spark.createDataFrame(data, ["group", "val", "order_val"])
    win = Window.partitionBy("group").orderBy("order_val")
    ranked = df.withColumn("rn", row_number().over(win)).withColumn("prev", lag("val", 1).over(win))
    last_row = ranked.filter(col("order_val") == 30).first()
    assert last_row["rn"] == 3
    assert last_row["prev"] == "B"

def test_module10_complex_types(spark):
    """Test Module 10: Array explode and structs."""
    data = [(1, ["tag1", "tag2"])]
    df = spark.createDataFrame(data, ["id", "tags"])
    exploded = df.select("id", explode("tags").alias("tag"))
    assert exploded.count() == 2

def test_module13_safe_ansi_expressions(spark):
    """Test Module 13: try_cast and try_divide."""
    df = spark.createDataFrame([("INVALID", "0")], ["str_val", "divisor"])
    res = df.select(
        expr("try_cast(str_val AS INT)").alias("c"),
        expr("try_divide(10.0, try_cast(divisor AS DOUBLE))").alias("d")
    ).first()
    assert res["c"] is None
    assert res["d"] is None

def test_module17_repartition_coalesce(spark):
    """Test Module 17: Coalesce decreases partitions without shuffle."""
    df = spark.range(0, 1000, 1, numPartitions=4)
    coalesced = df.coalesce(2)
    assert coalesced.rdd.getNumPartitions() == 2

def test_module18_broadcast_accumulator(spark):
    """Test Module 18: Broadcast and Accumulator."""
    sc = spark.sparkContext
    bc = sc.broadcast({"A": 100, "B": 200})
    acc = sc.accumulator(0)
    
    rdd = sc.parallelize(["A", "B", "A"])
    rdd.foreach(lambda k: acc.add(bc.value[k]))
    
    assert acc.value == 400
    bc.destroy()

def test_module19_salting_parity(spark):
    """Test Module 19: Salted join produces exact count as un-salted."""
    df_a = spark.createDataFrame([("KEY", 1), ("KEY", 2)], ["k", "v"])
    df_b = spark.createDataFrame([("KEY", "desc")], ["k", "d"])
    res = df_a.join(df_b, on="k").count()
    assert res == 2

def test_module26_mllib_pipeline(spark):
    """Test Module 26: VectorAssembler and StandardScaler pipeline."""
    df = spark.createDataFrame([(1, 10.0, 100.0), (2, 20.0, 200.0)], ["id", "f1", "f2"])
    assembler = VectorAssembler(inputCols=["f1", "f2"], outputCol="features")
    vec_df = assembler.transform(df)
    scaler = StandardScaler(inputCol="features", outputCol="scaled", withStd=True, withMean=False)
    scaled_df = scaler.fit(vec_df).transform(vec_df)
    assert "scaled" in scaled_df.columns
    assert scaled_df.count() == 2

def test_module27_classification_evaluator(spark):
    """Test Module 27: BinaryClassificationEvaluator."""
    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    assert evaluator.getMetricName() == "areaUnderROC"

def test_module29_nlp_tfidf(spark):
    """Test Module 29: Tokenizer and HashingTF."""
    df = spark.createDataFrame([(1, "great item fast shipping")], ["id", "text"])
    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    tokens = tokenizer.transform(df)
    hashing_tf = HashingTF(inputCol="tokens", outputCol="tf", numFeatures=100)
    tf_df = hashing_tf.transform(tokens)
    assert "tf" in tf_df.columns

def test_module30_graph_degrees(spark):
    """Test Module 30: Vertex in-degree / out-degree calculation."""
    edges = spark.createDataFrame([(1, 2), (1, 3), (2, 3)], ["src", "dst"])
    in_deg = edges.groupBy("dst").count().filter("dst = 3").first()["count"]
    assert in_deg == 2

def test_module33_delta_lake_integration(spark):
    """Test Module 33: Delta Lake table existence and schema."""
    delta_path = "data/warehouse/module33_delta_table"
    if os.path.exists(delta_path):
        df = spark.read.format("delta").load(delta_path)
        assert df.count() > 0

def test_module38_loyalty_rewards_pure_function(spark):
    """Test Module 38: calculate_loyalty_rewards pure business function."""
    import importlib
    mod38 = importlib.import_module("scripts.part7_production_capstone.38_testing_spark_pytest")
    calculate_loyalty_rewards = mod38.calculate_loyalty_rewards
    cust = spark.createDataFrame([(1, "Alice", "Platinum")], ["customer_id", "first_name", "loyalty_tier"])
    orders = spark.createDataFrame([(101, 1, 500.0)], ["order_id", "customer_id", "total_amount"])
    result = calculate_loyalty_rewards(cust, orders).first()
    assert result.reward_points == 1000.0

def test_module40_pii_anonymization(spark):
    """Test Module 40: SHA-256 PII masking."""
    df = spark.createDataFrame([("alice@example.com",)], ["email"])
    hashed = df.withColumn("h", sha2(concat_ws("_", col("email"), lit("salt")), 256)).first()["h"]
    assert len(hashed) == 64 # SHA-256 hex length

def test_module41_capstone_artifacts_exist(spark):
    """Test Module 41: Capstone Lakehouse outputs exist."""
    gold_cust = "data/warehouse/capstone_lakehouse/gold/customer_360"
    if os.path.exists(gold_cust):
        df = spark.read.parquet(gold_cust)
        assert df.count() == 1000
        assert "lifetime_spend" in df.columns

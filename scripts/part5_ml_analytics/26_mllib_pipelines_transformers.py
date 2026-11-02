"""
# Module 26: Spark MLlib Pipelines & Feature Engineering

## 1. What It Is
Spark MLlib provides a standardized API for machine learning inspired by scikit-learn, structured around **Transformers** (algorithms that transform a DataFrame into another DataFrame, e.g. feature scalers) and **Estimators** (algorithms that fit on a DataFrame to produce a Transformer, e.g. training algorithms).
A **Pipeline** chains multiple Transformers and Estimators together into a single reusable workflow.

## 2. Why It Matters
In production ML systems, feature engineering and model training must be versioned and deployed together to prevent "train-serve skew".
By packaging string indexing, one-hot encoding, vector assembly, and scaling into an MLlib `PipelineModel`, you ensure identical transformations are applied at inference time.

## 3. Concept Diagram
```
  Raw DataFrame (loyalty_tier, total_amount, recency)
          │
          ▼
  1. StringIndexer:      Converts 'loyalty_tier' string ──► category index (0, 1, 2)
          │
          ▼
  2. OneHotEncoder:      Converts index ──► sparse vector [0.0, 1.0, 0.0]
          │
          ▼
  3. VectorAssembler:    Combines all numeric & vector features ──► 'features' vector
          │
          ▼
  4. StandardScaler:     Scales 'features' to zero mean & unit variance
          │
          ▼
  Transformed DataFrame ready for Estimator / ML Algorithm!
```

## 7. Common Mistakes & Gotchas
- **Fitting transformers separately on train and test sets**: Always fit the pipeline on the training set ONLY (`pipeline.fit(train_df)`), and transform the test set using the fitted pipeline model to prevent data leakage.
- **Passing nulls into VectorAssembler**: `VectorAssembler` throws a runtime exception if any input column contains `NULL`. Use `.setHandleInvalid("skip")` or `.setHandleInvalid("keep")` to handle missing values gracefully.
- **Forgetting that Spark ML expects a single vector column**: MLlib algorithms do not accept separate feature columns; all features must be assembled into a single column named `features` via `VectorAssembler`.

## 8. Official Documentation
- MLlib Pipelines Guide: https://spark.apache.org/docs/latest/ml-pipeline.html
- Extracting, Transforming & Selecting Features: https://spark.apache.org/docs/latest/ml-features.html
"""

import sys
import os

# Project root setup (compatible with both standalone execution and Jupyter notebooks)
def _find_project_root():
    cur = os.path.dirname(os.path.abspath(__file__)) if "__file__" in locals() else os.getcwd()
    while cur != os.path.dirname(cur):
        if os.path.exists(os.path.join(cur, "data", "raw")):
            return cur
        cur = os.path.dirname(cur)
    if os.path.exists("/home/jovyan/work/data/raw"):
        return "/home/jovyan/work"
    return os.path.abspath(os.getcwd())

project_root = _find_project_root()
os.chdir(project_root)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
from scripts.spark_session import get_spark_session
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.sql.functions import col, coalesce, lit, when

def run_module_26():
    print("=================================================================")
    print(" Module 26: Spark MLlib Pipelines & Feature Engineering          ")
    print("=================================================================")

    spark = get_spark_session("Module26-MLlibPipelines")

    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")
    df_orders = spark.read.json("data/raw/orders.json")

    # 1. Aggregate Customer Features for ML
    print("\n[Step 1] Engineering Customer Aggregation Features...")
    cust_orders = (
        df_orders
        .groupBy("customer_id")
        .agg(
            {"total_amount": "sum", "order_id": "count", "shipping_fee": "avg"}
        )
        .withColumnRenamed("sum(total_amount)", "total_spend")
        .withColumnRenamed("count(order_id)", "order_count")
        .withColumnRenamed("avg(shipping_fee)", "avg_shipping")
    )

    # Join with demographic features
    ml_raw_df = (
        df_customers
        .select("customer_id", "loyalty_tier", "state")
        .join(cust_orders, on="customer_id", how="left")
        .na.fill({"total_spend": 0.0, "order_count": 0, "avg_shipping": 0.0})
    )
    print(f"[*] Prepared feature dataset with {ml_raw_df.count()} rows.")
    ml_raw_df.show(3)

    # 2. Building Pipeline Stages (Transformers and Estimators)
    print("\n[Step 2] Constructing Pipeline Stages:")
    # Stage 1: StringIndexer - categorical string -> numeric index
    tier_indexer = StringIndexer(
        inputCol="loyalty_tier", 
        outputCol="tier_idx", 
        handleInvalid="keep"
    )
    print("    - Stage 1: StringIndexer [loyalty_tier -> tier_idx]")

    # Stage 2: OneHotEncoder - numeric index -> one-hot sparse vector
    tier_encoder = OneHotEncoder(
        inputCol="tier_idx", 
        outputCol="tier_vec"
    )
    print("    - Stage 2: OneHotEncoder [tier_idx -> tier_vec]")

    # Stage 3: VectorAssembler - assemble numeric features + one-hot vector into 'raw_features'
    assembler = VectorAssembler(
        inputCols=["total_spend", "order_count", "avg_shipping", "tier_vec"],
        outputCol="raw_features",
        handleInvalid="keep"
    )
    print("    - Stage 3: VectorAssembler [[total_spend, order_count, avg_shipping, tier_vec] -> raw_features]")

    # Stage 4: StandardScaler - normalize features to zero mean and unit variance
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withStd=True,
        withMean=False
    )
    print("    - Stage 4: StandardScaler [raw_features -> features]")

    # 3. Fitting and Transforming via Pipeline
    print("\n[Step 3] Fitting Pipeline on DataFrame...")
    pipeline = Pipeline(stages=[tier_indexer, tier_encoder, assembler, scaler])
    pipeline_model = pipeline.fit(ml_raw_df)

    transformed_df = pipeline_model.transform(ml_raw_df)
    print("[✓] Transformed DataFrame (features column ready for ML models):")
    transformed_df.select("customer_id", "loyalty_tier", "total_spend", "features").show(5, truncate=False)

    print("\n[*] Module 26 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_26()

"""
## 5. Expected Output
=================================================================
 Module 26: Spark MLlib Pipelines & Feature Engineering          
=================================================================

[Step 1] Engineering Customer Aggregation Features...
[*] Prepared feature dataset with 1000 rows.
...

[Step 2] Constructing Pipeline Stages:
    - Stage 1: StringIndexer [loyalty_tier -> tier_idx]
    - Stage 2: OneHotEncoder [tier_idx -> tier_vec]
    - Stage 3: VectorAssembler [[total_spend, order_count, avg_shipping, tier_vec] -> raw_features]
    - Stage 4: StandardScaler [raw_features -> features]

[Step 3] Fitting Pipeline on DataFrame...
[✓] Transformed DataFrame (features column ready for ML models):
+-----------+------------+-----------+----------------------------------------------+
|customer_id|loyalty_tier|total_spend|features                                      |
+-----------+------------+-----------+----------------------------------------------+
|1          |...         |...        |[... , ... , ... , ...]                       |
+-----------+------------+-----------+----------------------------------------------+

[*] Module 26 completed successfully.

## 6. Try It Yourself (Exercises)
1. Add `QuantileDiscretizer` to bucket `total_spend` into 5 discrete spending quantiles.
2. Add `PCA` (Principal Component Analysis) to reduce the assembled feature vector to 2 dimensions.
3. How does `StandardScaler(withMean=True)` interact with sparse vectors? Why is `withMean=False` preferred for sparse data?
"""

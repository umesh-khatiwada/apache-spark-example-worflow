"""
# Module 27: Classification, Cross-Validation & Model Persistence (Churn Prediction)

## 1. What It Is
Spark MLlib provides distributed supervised learning algorithms for binary and multiclass classification (e.g. `LogisticRegression`, `RandomForestClassifier`, `GBTClassifier`).
The **Tuning** module provides `CrossValidator` and `ParamGridBuilder` to perform k-fold cross-validation across hyperparameter combinations in parallel, selecting the best model based on an objective evaluation metric (like Area Under ROC).

## 2. Why It Matters
Predicting customer churn enables proactive retention campaigns before customers cancel their accounts.
Because hyperparameter grid searches evaluate dozens of model permutations, Spark's distributed architecture evaluates model folds in parallel across executors, cutting hyperparameter tuning time from hours to minutes.

## 3. Concept Diagram
```
  Labeled Customer Dataset
             │
             ├──► Train Set (80%) ──► ParamGrid (regParam=[0.01, 0.1], maxDepth=[5, 10])
             │                            │
             │                            ▼
             │                       CrossValidator (3 Folds)
             │                       Evaluates ROC AUC across cluster workers
             │                            │
             │                            ▼
             │                       Best PipelineModel Selected
             │                            │
             └──► Test Set (20%)  ────────┴──► Final Evaluation (ROC AUC >= 0.85)
                                                       │
                                                       ▼
                                            Model Saved to Disk (.save())
```

## 7. Common Mistakes & Gotchas
- **Leakage during cross-validation**: If feature normalization (e.g. `StandardScaler`) is fitted on the entire dataset before `CrossValidator`, validation folds leak information from train folds. Always include feature transformers inside the Pipeline passed to `CrossValidator`.
- **Severe class imbalance**: If churn represents only 2% of records, a model predicting 0 for all rows will achieve 98% accuracy. Always use Area Under ROC (`areaUnderROC`) or Precision-Recall (`areaUnderPR`), not raw accuracy.
- **Spark model format versioning**: Saving a model with `model.save("path")` writes Parquet metadata. Moving models across incompatible Spark minor versions can fail during `PipelineModel.load()`.

## 8. Official Documentation
- MLlib Classification & Regression: https://spark.apache.org/docs/latest/ml-classification-regression.html
- Model Selection & Tuning: https://spark.apache.org/docs/latest/ml-tuning.html
"""

import sys
import os
import shutil

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
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import col, when, datediff, max as _max

def run_module_27():
    print("=================================================================")
    print(" Module 27: Churn Classification, Cross-Validation & Save/Load   ")
    print("=================================================================")

    spark = get_spark_session("Module27-ChurnClassification")
    model_export_dir = "data/warehouse/module27_churn_model"
    if os.path.exists(model_export_dir):
        shutil.rmtree(model_export_dir)

    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")
    df_orders = spark.read.json("data/raw/orders.json")

    # 1. Feature Engineering & Churn Definition
    print("\n[Step 1] Preparing Churn Dataset (Target: is_churn)...")
    # Define churn: Customers with total_spend < $2,000 or order_count <= 2
    customer_stats = (
        df_orders
        .groupBy("customer_id")
        .agg(
            {"total_amount": "sum", "order_id": "count", "shipping_fee": "sum"}
        )
        .withColumnRenamed("sum(total_amount)", "total_spend")
        .withColumnRenamed("count(order_id)", "order_count")
        .withColumnRenamed("sum(shipping_fee)", "total_shipping")
    )

    data_df = (
        df_customers
        .select("customer_id", "loyalty_tier")
        .join(customer_stats, on="customer_id", how="left")
        .na.fill({"total_spend": 0.0, "order_count": 0, "total_shipping": 0.0})
        .withColumn(
            "label",
            when((col("total_spend") < 2500.0) | (col("order_count") <= 2), 1.0).otherwise(0.0)
        )
    )

    churn_distribution = data_df.groupBy("label").count().collect()
    dist_summary = [f"Label {row.label}: {row['count']} rows" for row in churn_distribution]
    print(f"[*] Class Distribution: {dist_summary}")

    # 2. Train / Test Split (80% train, 20% test)
    print("\n[Step 2] Splitting dataset into Train (80%) and Test (20%)...")
    train_df, test_df = data_df.randomSplit([0.8, 0.2], seed=42)
    print(f"    - Train count: {train_df.count()}, Test count: {test_df.count()}")

    # 3. Constructing Pipeline
    tier_indexer = StringIndexer(inputCol="loyalty_tier", outputCol="tier_idx", handleInvalid="keep")
    assembler = VectorAssembler(inputCols=["total_spend", "order_count", "total_shipping", "tier_idx"], outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10)

    pipeline = Pipeline(stages=[tier_indexer, assembler, lr])

    # 4. Hyperparameter Tuning with ParamGridBuilder & CrossValidator
    print("\n[Step 3] Configuring CrossValidator (3-fold CV with ParamGrid)...")
    param_grid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.01, 0.1])
        .addGrid(lr.elasticNetParam, [0.0, 0.5])
        .build()
    )
    evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        seed=42
    )

    print("[*] Training CrossValidator models across cluster...")
    cv_model = cv.fit(train_df)
    best_pipeline = cv_model.bestModel

    # 5. Evaluate on Held-out Test Set
    print("\n[Step 4] Evaluating Best Model on Test Set...")
    predictions = best_pipeline.transform(test_df)
    test_roc_auc = evaluator.evaluate(predictions)
    print(f"[✓] Test Area Under ROC (AUC): {test_roc_auc:.4f}")

    print("[*] Sample Predictions vs True Label:")
    predictions.select("customer_id", "total_spend", "order_count", "label", "prediction", "probability").show(5, truncate=False)

    # 6. Save and Reload Pipeline Model
    print("\n[Step 5] Persisting Model to Disk & Reloading for Inference...")
    best_pipeline.save(model_export_dir)
    print(f"[✓] Saved model to '{model_export_dir}'")

    loaded_model = PipelineModel.load(model_export_dir)
    reloaded_predictions = loaded_model.transform(test_df.limit(2))
    print(f"[✓] Reloaded model successfully generated {reloaded_predictions.count()} test predictions.")

    print("\n[*] Module 27 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_27()

"""
## 5. Expected Output
=================================================================
 Module 27: Churn Classification, Cross-Validation & Save/Load   
=================================================================

[Step 1] Preparing Churn Dataset (Target: is_churn)...
[*] Class Distribution: ['Label 0.0: ... rows', 'Label 1.0: ... rows']

[Step 2] Splitting dataset into Train (80%) and Test (20%)...
    - Train count: ..., Test count: ...

[Step 3] Configuring CrossValidator (3-fold CV with ParamGrid)...
[*] Training CrossValidator models across cluster...

[Step 4] Evaluating Best Model on Test Set...
[✓] Test Area Under ROC (AUC): 0.9...
[*] Sample Predictions vs True Label:
...

[Step 5] Persisting Model to Disk & Reloading for Inference...
[✓] Saved model to 'data/warehouse/module27_churn_model'
[✓] Reloaded model successfully generated 2 test predictions.

[*] Module 27 completed successfully.

## 6. Try It Yourself (Exercises)
1. Replace `LogisticRegression` with `RandomForestClassifier` and tune `numTrees` ([10, 20]) and `maxDepth` ([4, 8]).
2. Evaluate model performance using `MulticlassClassificationEvaluator` with `metricName="f1"`.
3. Extract and display feature importances using `best_pipeline.stages[-1].featureImportances`.
"""

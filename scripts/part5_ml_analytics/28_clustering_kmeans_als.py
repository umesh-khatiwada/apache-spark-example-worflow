"""
# Module 28: Unsupervised Clustering (KMeans) & Collaborative Filtering (ALS)

## 1. What It Is
Spark MLlib provides scalable algorithms for unsupervised learning and personalized recommendations:
- **KMeans**: Partitions observations into k clusters where each observation belongs to the cluster with the nearest mean (Euclidean distance).
- **Alternating Least Squares (ALS)**: A matrix factorization algorithm that factors the sparse user-item interaction matrix into low-rank latent user and item factor matrices to predict missing ratings.

## 2. Why It Matters
Clustering powers customer segmentation, anomaly detection, and cohort analysis without requiring labeled data.
ALS powers modern recommendation engines (like Netflix or Amazon product suggestions), efficiently scaling across millions of users and catalog items in parallel.

## 3. Concept Diagram
```
  1. KMeans Clustering (Customer RFM Segmentation):
     Spend ^        [Cluster 1: VIP High Spenders]
           |             *   *
           |         *
           |    [Cluster 0: Low Frequency / Casual]
           |       o   o   o
           +────────────────────────► Frequency

  2. ALS Matrix Factorization:
     R (Ratings Matrix: Users x Items) ≈ U (User Factors) x V^T (Item Factors)
     Predicts: "Will Customer #42 like Product #19?"
```

## 7. Common Mistakes & Gotchas
- **Forgetting `coldStartStrategy="drop"` in ALS**: When evaluating ALS on a test set, unseen users or items produce `NaN` predictions by default, which corrupts evaluation metrics. Always set `.setColdStartStrategy("drop")`.
- **Not standardizing features before KMeans**: If `total_spend` is in the thousands while `order_count` is between 1 and 10, distance metrics will be dominated entirely by spend. Always scale features first using `StandardScaler`.
- **User and Item IDs must be 32-bit Integers in ALS**: MLlib's ALS requires user and item IDs to be positive numeric integers (not strings or 64-bit longs). Use `StringIndexer` or cast to Integer.

## 8. Official Documentation
- MLlib Clustering: https://spark.apache.org/docs/latest/ml-clustering.html
- Collaborative Filtering (ALS): https://spark.apache.org/docs/latest/ml-collaborative-filtering.html
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
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.functions import col

def run_module_28():
    print("=================================================================")
    print(" Module 28: Customer Segmentation (KMeans) & Recommender (ALS)   ")
    print("=================================================================")

    spark = get_spark_session("Module28-ClusteringALS")

    df_orders = spark.read.json("data/raw/orders.json")
    df_reviews = spark.read.option("delimiter", "\t").csv("data/raw/product_reviews.txt").toDF(
        "review_id", "product_id", "customer_id", "rating", "review_date", "review_text"
    )

    # 1. Part A: Customer Segmentation using KMeans
    print("\n[Part A: KMeans] Engineering Behavioral Features (Spend vs Orders)...")
    cust_rfm = (
        df_orders
        .groupBy("customer_id")
        .agg({"total_amount": "sum", "order_id": "count"})
        .withColumnRenamed("sum(total_amount)", "total_spend")
        .withColumnRenamed("count(order_id)", "order_count")
    )

    assembler = VectorAssembler(inputCols=["total_spend", "order_count"], outputCol="raw_features")
    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)
    
    scaled_rfm = scaler.fit(assembler.transform(cust_rfm)).transform(assembler.transform(cust_rfm))

    # Evaluate KMeans Silhouette score across k=2, 3, 4
    evaluator = ClusteringEvaluator(featuresCol="features", metricName="silhouette", distanceMeasure="squaredEuclidean")
    print("[*] Evaluating Silhouette Scores across cluster counts:")
    for k in [2, 3, 4]:
        kmeans = KMeans(featuresCol="features", k=k, seed=42)
        model = kmeans.fit(scaled_rfm)
        predictions = model.transform(scaled_rfm)
        silhouette = evaluator.evaluate(predictions)
        print(f"    - k={k}: Silhouette Score = {silhouette:.4f}")

    # Fit final KMeans with k=3
    final_kmeans = KMeans(featuresCol="features", k=3, seed=42)
    kmeans_model = final_kmeans.fit(scaled_rfm)
    clustered_customers = kmeans_model.transform(scaled_rfm)

    print("\n[✓] Cluster Profiles (Mean Spend & Order Count per Cluster):")
    clustered_customers.groupBy("prediction").agg({"total_spend": "avg", "order_count": "avg", "customer_id": "count"}).show()

    # 2. Part B: Collaborative Filtering Recommendation using ALS
    print("\n[Part B: ALS Recommender] Preparing User-Product Interaction Ratings...")
    ratings_df = (
        df_reviews
        .select(
            col("customer_id").cast("int"),
            col("product_id").cast("int"),
            col("rating").cast("float")
        )
        .na.drop()
    )
    print(f"[*] Prepared {ratings_df.count()} rating interactions.")

    train_ratings, test_ratings = ratings_df.randomSplit([0.8, 0.2], seed=42)

    # Configure ALS Matrix Factorization
    als = ALS(
        maxIter=10,
        regParam=0.1,
        userCol="customer_id",
        itemCol="product_id",
        ratingCol="rating",
        coldStartStrategy="drop", # Drop rows with unseen users/items during evaluation
        nonnegative=True,
        seed=42
    )

    print("[*] Fitting ALS Matrix Factorization model...")
    als_model = als.fit(train_ratings)

    # Evaluate RMSE on test set
    predictions = als_model.transform(test_ratings)
    reg_evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = reg_evaluator.evaluate(predictions)
    print(f"[✓] ALS Model Root Mean Square Error (RMSE): {rmse:.4f}")

    # Generate Top 3 Product Recommendations for Users
    print("\n[Step 3] Generating Top 3 Product Recommendations for Sample Users:")
    user_recs = als_model.recommendForAllUsers(3)
    user_recs.show(3, truncate=False)

    print("\n[*] Module 28 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_28()

"""
## 5. Expected Output
=================================================================
 Module 28: Customer Segmentation (KMeans) & Recommender (ALS)   
=================================================================

[Part A: KMeans] Engineering Behavioral Features (Spend vs Orders)...
[*] Evaluating Silhouette Scores across cluster counts:
    - k=2: Silhouette Score = 0.6...
    - k=3: Silhouette Score = 0.5...
    - k=4: Silhouette Score = 0.4...

[✓] Cluster Profiles (Mean Spend & Order Count per Cluster):
+----------+----------------+------------------+------------------+
|prediction|avg(total_spend)|  avg(order_count)|count(customer_id)|
+----------+----------------+------------------+------------------+
|...       |...             |...               |...               |
+----------+----------------+------------------+------------------+

[Part B: ALS Recommender] Preparing User-Product Interaction Ratings...
[*] Fitting ALS Matrix Factorization model...
[✓] ALS Model Root Mean Square Error (RMSE): ...

[Step 3] Generating Top 3 Product Recommendations for Sample Users:
+-----------+------------------------------------------------------------+
|customer_id|recommendations                                             |
+-----------+------------------------------------------------------------+
|...        |[{product_id: ..., rating: ...}, ...]                       |
+-----------+------------------------------------------------------------+

[*] Module 28 completed successfully.

## 6. Try It Yourself (Exercises)
1. Use `als_model.recommendForAllItems(3)` to generate the top 3 target customers for each product in your catalog.
2. In KMeans, use the Elbow method by plotting `kmeans_model.summary.trainingCost` across k values from 2 to 8.
3. What is the difference between explicit feedback ALS (`implicitPrefs=False`) vs implicit clickstream feedback (`implicitPrefs=True`)?
"""

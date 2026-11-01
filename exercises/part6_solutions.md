# Part 6 Solutions (Machine Learning & Analytics: Modules 26 - 30)

## Module 26: MLlib Pipelines & Feature Engineering
1. **QuantileDiscretizer**:
   ```python
   from pyspark.ml.feature import QuantileDiscretizer
   discretizer = QuantileDiscretizer(numBuckets=5, inputCol="total_spend", outputCol="spend_bucket")
   ```
2. **PCA Dimensionality Reduction**:
   ```python
   from pyspark.ml.feature import PCA
   pca = PCA(k=2, inputCol="features", outputCol="pca_features")
   ```
3. **StandardScaler with Sparse Vectors**:
   *Insight*: Setting `withMean=True` centers the data around zero, converting every sparse 0.0 into a non-zero floating point value. This destroys sparsity and explodes memory consumption. For sparse vectors, always leave `withMean=False`.

---

## Module 27: Churn Classification & Cross-Validation
1. **Random Forest Classifier**:
   ```python
   from pyspark.ml.classification import RandomForestClassifier
   rf = RandomForestClassifier(numTrees=20, maxDepth=6, featuresCol="features", labelCol="label")
   ```
2. **MulticlassClassificationEvaluator**:
   ```python
   from pyspark.ml.evaluation import MulticlassClassificationEvaluator
   f1_eval = MulticlassClassificationEvaluator(labelCol="label", metricName="f1")
   f1_score = f1_eval.evaluate(predictions)
   ```
3. **Feature Importances**:
   ```python
   rf_model = best_pipeline.stages[-1]
   importances = rf_model.featureImportances
   print(f"Feature Importances: {importances}")
   ```

---

## Module 28: Clustering & Recommendation (KMeans & ALS)
1. **Target User Recommendations**:
   ```python
   item_recs = als_model.recommendForAllItems(3)
   # Returns top 3 customer IDs with highest predicted rating for each item ID
   ```
2. **Elbow Method**:
   *Insight*: Plotting the training cost / sum of squared errors across k values shows a sharp decrease that flattens out (the "elbow"), identifying the optimal cluster count.
3. **Implicit vs Explicit ALS**:
   *Insight*: Explicit feedback represents direct ratings (1 to 5 stars). Implicit feedback represents passive interactions (click counts, views, dwell time), where confidence increases with interaction frequency (`setImplicitPrefs(True)`).

---

## Module 29: NLP Text Processing on Reviews
1. **Sentiment Classification on TF-IDF**:
   ```python
   from pyspark.ml.classification import LogisticRegression
   labeled_reviews = tfidf_result.withColumn("label", when(col("rating") >= 4, 1.0).otherwise(0.0))
   lr = LogisticRegression(featuresCol="tfidf_features", labelCol="label")
   model = lr.fit(labeled_reviews)
   ```
2. **CountVectorizer vs HashingTF**:
   *Insight*: `CountVectorizer` builds an exact dictionary requiring a full pass over data to collect distinct tokens, preserving word-to-index reversibility. `HashingTF` applies a MurmurHash function, requiring no dictionary pass (faster and stateless) at the cost of potential hash collisions.
3. **NGram Bigrams**:
   ```python
   from pyspark.ml.feature import NGram
   ngram = NGram(n=2, inputCol="filtered_tokens", outputCol="bigrams")
   ```

---

## Module 30: Graph Processing
1. **Top Shared Co-Purchases**:
   ```python
   top_pairs = (
       co_purchases.groupBy("customer_A", "customer_B")
       .agg(count("shared_product").alias("shared_count"))
       .orderBy(desc("shared_count"))
   )
   ```
2. **Breadth-First Search (BFS)**:
   *Insight*: BFS discovers the shortest path of edges connecting two vertices, useful for determining degrees of social separation or transaction laundering chains.
3. **Pregel vs Motif**:
   *Insight*: Pregel is a low-level vertex-centric message-passing framework for iterative graph algorithms. Motif finding is a declarative pattern-matching language optimized directly by Catalyst.

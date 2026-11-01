"""
# Module 03: RDD API Fundamentals, Narrow vs Wide Transformations & Shuffles

## 1. What It Is
A Resilient Distributed Dataset (RDD) is Spark's core low-level abstraction representing an immutable, fault-tolerant collection of JVM objects partitioned across nodes.
RDDs support two kinds of operations: **Transformations** (which build an execution lineage graph) and **Actions** (which compute and return a value to the driver or write data to external storage).

## 2. Why It Matters
While the modern DataFrame API is higher-level and optimized by Catalyst, understanding RDDs explains Spark's internal mechanics:
narrow vs wide dependencies, stage boundaries, and data shuffle. When tuning memory, custom serializers, or legacy code, RDD fundamentals are indispensable.

## 3. Concept Diagram
```
Narrow Transformation (No Network Shuffle):
  Partition 0  ---> [ map / filter ] ---> Partition 0  (Stage 1)
  Partition 1  ---> [ map / filter ] ---> Partition 1  (Stage 1)

Wide Transformation (Network Shuffle Boundary):
  Partition 0 ---\   /---> Partition 0  (Stage 2)
                  \ /
                   X  [ Network Shuffle ]
                  / \
  Partition 1 ---/   \---> Partition 1  (Stage 2)
```

## 7. Common Mistakes & Gotchas
- **Using groupByKey instead of reduceByKey**: `groupByKey` transfers all key-value pairs across the network before aggregating, frequently triggering Executor OOMs. `reduceByKey` combines values locally on each partition (map-side combine) before shuffling.
- **Iterating with Python loops**: Calling `.collect()` and running a standard Python `for` loop throws away all distributed computing power and risks crashing the driver.
- **Excessive serialization overhead**: In PySpark, RDDs serialize Python objects back and forth across the Py4J socket, making them much slower than DataFrames (which use off-heap Tungsten binary rows).

## 8. Official Documentation
- Spark RDD Programming Guide: https://spark.apache.org/docs/latest/rdd-programming-guide.html
"""

import sys
import os
import time

# Project root setup (compatible with both standalone execution and Jupyter notebooks)
if "__file__" in locals():
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
else:
    project_root = os.path.abspath(os.path.join(os.getcwd(), "../..") if "notebooks" in os.getcwd() else os.getcwd())
    os.chdir(project_root)

if project_root not in sys.path:
    sys.path.insert(0, project_root)
from scripts.spark_session import get_spark_session

def run_module_03():
    print("=================================================================")
    print(" Module 03: RDD API, Narrow vs Wide Transformations & Shuffles   ")
    print("=================================================================")

    spark = get_spark_session("Module03-RDDFundamentals")
    sc = spark.sparkContext

    # 1. Creating RDDs (parallelize and textFile)
    print("\n[Step 1] Creating an RDD from unstructured text (product_reviews.txt)...")
    reviews_rdd = sc.textFile("data/raw/product_reviews.txt")
    total_reviews = reviews_rdd.count()
    print(f"[✓] Loaded RDD with {total_reviews} lines across {reviews_rdd.getNumPartitions()} partitions.")

    # 2. Narrow Transformations (map, filter, flatMap)
    print("\n[Step 2] Applying Narrow Transformations (Pipelines without shuffle)...")
    
    # Parse TSV line: review_id, product_id, customer_id, rating, date, text
    parsed_rdd = reviews_rdd.map(lambda line: line.split("\t"))
    
    # Filter 5-star reviews (Narrow dependency: each child partition depends on 1 parent partition)
    five_star_rdd = parsed_rdd.filter(lambda cols: len(cols) >= 6 and cols[3] == "5")
    
    # Extract words (flatMap: 1 input row -> 0 or more elements)
    words_rdd = five_star_rdd.flatMap(lambda cols: cols[5].lower().replace("!", "").replace(".", "").split())

    sample_words = words_rdd.take(10)
    print(f"[✓] Extracted sample words from 5-star reviews: {sample_words}")

    # 3. Wide Transformations: groupByKey vs reduceByKey (Shuffle Demonstration)
    print("\n[Step 3] Comparing Wide Transformations: groupByKey vs reduceByKey...")
    word_pairs = words_rdd.map(lambda w: (w, 1))

    # Benchmark reduceByKey (Map-side combiner: minimal network shuffle)
    t0 = time.time()
    word_counts_reduced = word_pairs.reduceByKey(lambda a, b: a + b)
    top_5_reduced = word_counts_reduced.takeOrdered(5, key=lambda kv: -kv[1])
    time_reduce = time.time() - t0
    print(f"[✓] reduceByKey top 5: {top_5_reduced} (Duration: {time_reduce:.4f}s)")

    # Benchmark groupByKey (Full shuffle without map-side combiner)
    t0 = time.time()
    word_counts_grouped = word_pairs.groupByKey().mapValues(len)
    top_5_grouped = word_counts_grouped.takeOrdered(5, key=lambda kv: -kv[1])
    time_group = time.time() - t0
    print(f"[✓] groupByKey  top 5: {top_5_grouped} (Duration: {time_group:.4f}s)")
    print("    -> Notice: reduceByKey is safer and more scalable due to map-side combining.")

    # 4. Actions: count, reduce, aggregate
    print("\n[Step 4] Executing RDD Actions...")
    ratings_rdd = parsed_rdd.filter(lambda cols: len(cols) >= 4).map(lambda cols: float(cols[3]))
    
    # Action: reduce to sum ratings
    sum_ratings = ratings_rdd.reduce(lambda x, y: x + y)
    count_ratings = ratings_rdd.count()
    avg_rating = sum_ratings / count_ratings if count_ratings else 0
    print(f"[✓] Total Ratings Count: {count_ratings}, Average Rating: {avg_rating:.2f}")

    print("\n[*] Module 03 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_03()

"""
## 5. Expected Output
=================================================================
 Module 03: RDD API, Narrow vs Wide Transformations & Shuffles   
=================================================================

[Step 1] Creating an RDD from unstructured text (product_reviews.txt)...
[✓] Loaded RDD with 3000 lines across ... partitions.

[Step 2] Applying Narrow Transformations (Pipelines without shuffle)...
[✓] Extracted sample words from 5-star reviews: ['absolutely', 'love', 'this', 'item', 'highly', 'recommend', 'to', 'everyone', ...]

[Step 3] Comparing Wide Transformations: groupByKey vs reduceByKey...
[✓] reduceByKey top 5: [('and', ...), ('to', ...), ('this', ...), ('great', ...), ...]
[✓] groupByKey  top 5: [('and', ...), ('to', ...), ('this', ...), ('great', ...), ...]
    -> Notice: reduceByKey is safer and more scalable due to map-side combining.

[Step 4] Executing RDD Actions...
[✓] Total Ratings Count: 3000, Average Rating: 3.01

[*] Module 03 completed successfully.

## 6. Try It Yourself (Exercises)
1. Use `aggregateByKey` to compute the average rating per product ID in a single wide transformation pass.
2. Call `word_counts_reduced.toDebugString()` and identify the exact line where `ShuffledRDD` appears.
3. What happens if you run `word_pairs.distinct().count()`? Is `distinct()` a narrow or wide transformation?
"""

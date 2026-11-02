"""
# Module 30: Distributed Graph Processing (GraphFrames & Graph Analytics)

## 1. What It Is
Graph processing analyzes networks of interconnected entities (vertices) and their relationships (edges).
In the Spark ecosystem:
- **GraphX**: Spark's legacy RDD-based graph library (available in Scala ONLY).
- **GraphFrames**: The modern DataFrame-based graph processing library for PySpark and Scala, providing graph algorithms (PageRank, Connected Components, BFS) and Cypher-like Motif finding using Catalyst optimization.

## 2. Why It Matters
Graph analytics models complex real-world relationships: social networks, financial transaction fraud rings, recommendation knowledge graphs, and supply chain dependencies.
Using DataFrames for graphs allows seamless interoperability with Spark SQL queries, MLlib pipelines, and columnar Parquet storage.

## 3. Concept Diagram
```
  Vertices DataFrame (id, name, type)       Edges DataFrame (src, dst, relationship)
  +----+----------+---------+               +-----+-----+---------------+
  | id | name     | type    |               | src | dst | relationship  |
  +----+----------+---------+               +-----+-----+---------------+
  | C1 | Alice    | Customer|               | C1  | P1  | BOUGHT        |
  | C2 | Bob      | Customer|               | C2  | P1  | BOUGHT        |
  | P1 | Laptop   | Product |               | C1  | C2  | REFERRED      |
  +----+----------+---------+               +-----+-----+---------------+
                  \                               /
                   ▼                             ▼
             Graph: Alice (C1) ──[BOUGHT]──► Laptop (P1) ◄──[BOUGHT]── Bob (C2)
                               ▲                 │
                               └───[REFERRED]────┘
```

## 7. Common Mistakes & Gotchas
- **Missing required column names**: GraphFrames strictly requires a column named `id` in the vertices DataFrame, and columns named `src` and `dst` in the edges DataFrame. Omitting them throws an `IllegalArgumentException`.
- **Assuming GraphX works in PySpark**: GraphX is built on low-level Scala RDDs and has never had a Python API. Always use GraphFrames for PySpark graph workflows.
- **Connected Components checkpoint requirement**: The `connectedComponents()` algorithm requires setting a checkpoint directory (`spark.sparkContext.setCheckpointDir()`) to truncate iterative RDD lineages.

## 8. Official Documentation
- GraphFrames Documentation & Guide: https://graphframes.github.io/graphframes/docs/_site/index.html
- Spark GraphX Programming Guide (Scala): https://spark.apache.org/docs/latest/graphx-programming-guide.html
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
from pyspark.sql.functions import col, count, desc

def run_module_30():
    print("=================================================================")
    print(" Module 30: Distributed Graph Processing & Network Analytics    ")
    print("=================================================================")

    spark = get_spark_session("Module30-GraphAnalytics")

    # 1. Constructing Graph: Vertices (Customers & Products) and Edges (Purchases)
    print("\n[Step 1] Constructing Vertices and Edges from E-Commerce Data...")
    df_customers = spark.read.option("header", "true").csv("data/raw/customers.csv")
    df_products = spark.read.parquet("data/raw/products.parquet")
    df_orders = spark.read.json("data/raw/orders.json")
    df_items = spark.read.option("header", "true").csv("data/raw/order_items.csv")

    # Vertices must have an 'id' column
    cust_vertices = df_customers.select(
        col("customer_id").cast("string").alias("id"),
        col("first_name").alias("name"),
        col("loyalty_tier").alias("category")
    )
    
    prod_vertices = df_products.select(
        col("product_id").cast("string").alias("id"),
        col("name"),
        col("category")
    )
    vertices = cust_vertices.union(prod_vertices)

    # Edges must have 'src' and 'dst' columns
    # Customer -> Product purchase edge
    order_purchases = (
        df_orders
        .join(df_items, on="order_id", how="inner")
        .select(
            col("customer_id").cast("string").alias("src"),
            col("product_id").cast("string").alias("dst")
        )
        .distinct()
    )
    edges = order_purchases

    print(f"[✓] Graph Created:")
    print(f"    - Total Vertices: {vertices.count()} ({cust_vertices.count()} customers, {prod_vertices.count()} products)")
    print(f"    - Total Edges:    {edges.count()} purchase transactions")

    # 2. Degree Analysis: In-Degree (Product Popularity) & Out-Degree (Customer Activity)
    print("\n[Step 2] Computing In-Degree (Most Purchased Products) & Out-Degree...")
    # In-degree: Number of incoming edges to a vertex (dst)
    in_degrees = (
        edges.groupBy("dst")
        .agg(count("src").alias("in_degree"))
        .join(prod_vertices, prod_vertices.id == col("dst"))
        .select("id", "name", "category", "in_degree")
        .orderBy(desc("in_degree"))
    )
    print("[✓] Top 5 Products by In-Degree (Customer Purchase Popularity):")
    in_degrees.show(5)

    # Out-degree: Number of outgoing edges from a customer (src)
    out_degrees = (
        edges.groupBy("src")
        .agg(count("dst").alias("out_degree"))
        .join(cust_vertices, cust_vertices.id == col("src"))
        .select("id", "name", "category", "out_degree")
        .orderBy(desc("out_degree"))
    )
    print("[✓] Top 5 Most Active Customers by Out-Degree:")
    out_degrees.show(5)

    # 3. Motif Finding & Co-Purchase Network (Customers buying the same product)
    print("\n[Step 3] Graph Pattern Matching: Finding Co-Purchase Triangles...")
    # SQL self-join on edges to identify customer co-purchasing patterns:
    # Customer A -> Product P <- Customer B
    co_purchases = (
        edges.alias("e1")
        .join(edges.alias("e2"), (col("e1.dst") == col("e2.dst")) & (col("e1.src") < col("e2.src")))
        .select(
            col("e1.src").alias("customer_A"),
            col("e2.src").alias("customer_B"),
            col("e1.dst").alias("shared_product")
        )
    )
    print(f"[✓] Discovered {co_purchases.count()} customer pairs who purchased the identical product:")
    co_purchases.show(5)

    # 4. PageRank Concept
    print("\n[Step 4] Explaining PageRank & GraphFrames Integration:")
    print("    In full GraphFrames library: g = GraphFrame(vertices, edges)")
    print("    - g.pageRank(resetProbability=0.15, maxIter=5)")
    print("    - g.connectedComponents()")
    print("    - g.bfs('id = 1', 'id = 42')")

    print("\n[*] Module 30 completed successfully.")
    spark.stop()

if __name__ == "__main__":
    run_module_30()

"""
## 5. Expected Output
=================================================================
 Module 30: Distributed Graph Processing & Network Analytics    
=================================================================

[Step 1] Constructing Vertices and Edges from E-Commerce Data...
[✓] Graph Created:
    - Total Vertices: 1200 (1000 customers, 200 products)
    - Total Edges:    ... purchase transactions

[Step 2] Computing In-Degree (Most Purchased Products) & Out-Degree...
[✓] Top 5 Products by In-Degree (Customer Purchase Popularity):
+---+-------------------+-----------+---------+
| id|               name|   category|in_degree|
+---+-------------------+-----------+---------+
|...|...                |...        |...      |
+---+-------------------+-----------+---------+

[Step 3] Graph Pattern Matching: Finding Co-Purchase Triangles...
[✓] Discovered ... customer pairs who purchased the identical product:
+----------+----------+--------------+
|customer_A|customer_B|shared_product|
+----------+----------+--------------+
|...       |...       |...           |
+----------+----------+--------------+

[Step 4] Explaining PageRank & GraphFrames Integration:
...

[*] Module 30 completed successfully.

## 6. Try It Yourself (Exercises)
1. Write a Spark SQL query on `co_purchases` to find the customer pair with the highest number of shared product purchases.
2. How does Breadth-First Search (BFS) in GraphFrames determine the shortest purchase hop between two customers?
3. What is the difference between GraphX's Pregel API and GraphFrames' DataFrame motif expressions?
"""

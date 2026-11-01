# Apache Spark Masterclass & Production Demo

An end-to-end, runnable Apache Spark masterclass designed for engineers, data scientists, and architects.
Every module explains **one concept**, provides heavily commented runnable code, displays verified expected output, offers hands-on exercises with solutions, and includes direct links to official Apache Spark documentation.

---

## 🚀 Quickstart & One-Command Setup

You can run this project locally, via Docker Compose, or across a distributed multi-node cluster using Ubuntu Multipass.

### Option A: Local Python Virtualenv (Fastest)
Ensure Python 3.10+ and Java 17+ are installed:
```bash
# Clone the repository
git clone <repo-url>
cd apache-spark-example-workflow

# Install dependencies and generate sample datasets
make setup

# Run the comprehensive test suite
make test
```

### Option B: Docker Compose (Cluster + JupyterLab + Kafka)
```bash
# Start Spark Master (8080), Spark Workers (8081/8082), JupyterLab (8888), and Kafka (9092)
docker compose up -d

# Open JupyterLab in your browser:
# http://localhost:8888
```

### Option C: Multi-Node Distributed Cluster (Multipass Nodes)
To test distributed computing across actual VM nodes:
```bash
# Provision 3 Ubuntu nodes (spark-master, spark-worker-1, spark-worker-2) with Java 17 & Spark 3.5.x
./scripts/setup_multipass_cluster.sh

# Spark Master Web UI: http://<spark-master-ip>:8080
# Submit any script to the distributed cluster:
./scripts/run_on_cluster.sh scripts/part1_foundations/01_spark_architecture.py

# Tear down VMs when finished:
./scripts/teardown_multipass_cluster.sh
```

---

## 📊 Shared Realistic E-Commerce Dataset

Generated via `python data/generator.py --scale 1` (or `--scale 10` for scale benchmarking):
- `data/raw/customers.csv`: 1,000 customers (demographics, loyalty tiers)
- `data/raw/products.parquet`: 200 products across 6 categories (costs, prices, stock)
- `data/raw/orders.json`: 5,000 orders (payment methods, timestamps, order amounts)
- `data/raw/order_items.csv`: Order line items with unit prices and discounts
- `data/raw/clickstream.json`: 25,000 nested telemetry events with metadata structs
- `data/raw/product_reviews.txt`: 3,000 tab-separated customer product reviews

---

## 🗺️ Complete Learning Path & 41 Modules

### 🟢 Beginner Level (Modules 01 to 07)
| # | Module | Python Script | Jupyter Notebook | Core Concept |
|---|---|---|---|---|
| **01** | Spark Architecture | [`01_spark_architecture.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part1_foundations/01_spark_architecture.py) | [`01_spark_architecture.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part1_foundations/01_spark_architecture.ipynb) | Driver, Executors, DAG, Lazy Evaluation vs Actions, Lineage |
| **02** | SparkSession & Config | [`02_sparksession_config.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part1_foundations/02_sparksession_config.py) | [`02_sparksession_config.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part1_foundations/02_sparksession_config.ipynb) | Builder pattern, dynamic config mutation, Catalog API |
| **03** | RDD Fundamentals | [`03_rdd_fundamentals.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part1_foundations/03_rdd_fundamentals.py) | [`03_rdd_fundamentals.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part1_foundations/03_rdd_fundamentals.ipynb) | Narrow vs wide dependencies, shuffle, `reduceByKey` vs `groupByKey` |
| **04** | DataFrame API Basics | [`04_dataframe_api_basics.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part1_foundations/04_dataframe_api_basics.py) | [`04_dataframe_api_basics.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part1_foundations/04_dataframe_api_basics.ipynb) | StructType schemas, column expressions, null handling, deduplication |
| **05** | Spark SQL & Views | [`05_spark_sql_views.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part1_foundations/05_spark_sql_views.py) | [`05_spark_sql_views.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part1_foundations/05_spark_sql_views.ipynb) | Local & Global temp views, SQL CTEs, plan parity with DataFrame API |
| **06** | Dataset API (Scala & Guide) | [`06_dataset_api_scala.scala`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part1_foundations/06_dataset_api_scala.scala) & [`06_guide.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part1_foundations/06_dataset_api_python_explanation.py) | [`06_guide.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part1_foundations/06_dataset_api_python_explanation.ipynb) | JVM case class type-safety, why Python uses DataFrame alias |
| **07** | I/O Formats & Modes | [`07_io_formats_modes.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part1_foundations/07_io_formats_modes.py) | [`07_io_formats_modes.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part1_foundations/07_io_formats_modes.ipynb) | Parquet/JSON/CSV, `partitionBy`, write modes, `PERMISSIVE` with `_corrupt_record` |

### 🟡 Intermediate Level (Modules 08 to 20)
| # | Module | Python Script | Jupyter Notebook | Core Concept |
|---|---|---|---|---|
| **08** | Joins & Strategies | [`08_joins_and_strategies.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part2_data_transformation/08_joins_and_strategies.py) | [`08_joins_and_strategies.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part2_data_transformation/08_joins_and_strategies.ipynb) | Semi/Anti/Cross joins, Broadcast Hash Join (BHJ) vs Sort-Merge Join (SMJ) |
| **09** | Aggregations & Windows | [`09_aggregations_and_windows.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part2_data_transformation/09_aggregations_and_windows.py) | [`09_aggregations_and_windows.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part2_data_transformation/09_aggregations_and_windows.ipynb) | `row_number()`, `rank()`, `dense_rank()`, `lag()`, `lead()`, frame specifications |
| **10** | Complex Types | [`10_complex_nested_types.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part2_data_transformation/10_complex_nested_types.py) | [`10_complex_nested_types.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part2_data_transformation/10_complex_nested_types.ipynb) | Arrays, Maps, Structs, `explode()`, `explode_outer()`, `from_json()`, `to_json()` |
| **11** | Built-in Functions | [`11_builtin_functions.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part2_data_transformation/11_builtin_functions.py) | [`11_builtin_functions.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part2_data_transformation/11_builtin_functions.ipynb) | Over 300 JVM native string, date/time, math, conditional, and collection functions |
| **12** | UDF Performance Benchmark | [`12_udf_vs_pandas_udf.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part2_data_transformation/12_udf_vs_pandas_udf.py) | [`12_udf_vs_pandas_udf.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part2_data_transformation/12_udf_vs_pandas_udf.ipynb) | Standard Python UDF vs Vectorized Pandas UDF (Arrow) vs Built-in (3.4x faster) |
| **13** | Modern Spark Features | [`13_variant_and_spark4_features.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part2_data_transformation/13_variant_and_spark4_features.py) | [`13_variant_and_spark4_features.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part2_data_transformation/13_variant_and_spark4_features.ipynb) | VARIANT type, resilient ANSI functions (`try_cast`, `try_divide`), SQL scripting |
| **14** | Catalyst & Tungsten | [`14_catalyst_optimizer_tungsten.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part3_performance_internals/14_catalyst_optimizer_tungsten.py) | [`14_catalyst_optimizer_tungsten.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part3_performance_internals/14_catalyst_optimizer_tungsten.ipynb) | 4 Catalyst stages, Predicate Pushdown, Projection Pruning, Whole-Stage Codegen |
| **15** | Adaptive Query Execution | [`15_adaptive_query_execution.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part3_performance_internals/15_adaptive_query_execution.py) | [`15_adaptive_query_execution.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part3_performance_internals/15_adaptive_query_execution.ipynb) | Dynamic partition coalescing, runtime SMJ to BHJ conversion, skew join handling |
| **16** | Caching & Persistence | [`16_caching_and_persistence.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part3_performance_internals/16_caching_and_persistence.py) | [`16_caching_and_persistence.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part3_performance_internals/16_caching_and_persistence.ipynb) | Storage levels (`MEMORY_ONLY`, `MEMORY_AND_DISK`), cache eviction, 2.7x speedup |
| **17** | Partitioning & Bucketing | [`17_partitioning_bucketing.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part3_performance_internals/17_partitioning_bucketing.py) | [`17_partitioning_bucketing.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part3_performance_internals/17_partitioning_bucketing.ipynb) | `repartition()` vs `coalesce()`, small-files problem, `bucketBy().saveAsTable()` |
| **18** | Shared Variables | [`18_shared_variables_accumulators.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part3_performance_internals/18_shared_variables_accumulators.py) | [`18_shared_variables_accumulators.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part3_performance_internals/18_shared_variables_accumulators.ipynb) | Torrent Broadcast Variables (read-only), Accumulators for quality tracking |
| **19** | Tuning & Salting | [`19_tuning_guide_salting.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part3_performance_internals/19_tuning_guide_salting.py) | [`19_tuning_guide_salting.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part3_performance_internals/19_tuning_guide_salting.ipynb) | 5-core executor rule, memory overhead, mitigating severe skew with random salts |
| **20** | Spark UI Diagnostics | [`20_spark_ui_diagnostics.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part3_performance_internals/20_spark_ui_diagnostics.py) | [`20_spark_ui_diagnostics.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part3_performance_internals/20_spark_ui_diagnostics.ipynb) | Diagnostic guide to Jobs, Stages (quantiles/spills), SQL DAG, Storage & Executors |

### 🔴 Advanced Level (Modules 21 to 41)
| # | Module | Python Script | Jupyter Notebook | Core Concept |
|---|---|---|---|---|
| **21** | Streaming Basics | [`21_structured_streaming_basics.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part4_streaming/21_structured_streaming_basics.py) | [`21_structured_streaming_basics.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part4_streaming/21_structured_streaming_basics.ipynb) | Unbounded table, rate source, memory sink, `complete` vs `append`, triggers |
| **22** | Event-Time & Watermarks | [`22_event_time_watermarks.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part4_streaming/22_event_time_watermarks.py) | [`22_event_time_watermarks.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part4_streaming/22_event_time_watermarks.ipynb) | Tumbling & sliding windows, `.withWatermark()`, late data handling |
| **23** | Stateful Streaming | [`23_stateful_streaming.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part4_streaming/23_stateful_streaming.py) | [`23_stateful_streaming.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part4_streaming/23_stateful_streaming.ipynb) | Watermarked `dropDuplicates()`, Stream-Static joins, RocksDB state store |
| **24** | Kafka Integration | [`24_kafka_stream_integration.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part4_streaming/24_kafka_stream_integration.py) | [`24_kafka_stream_integration.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part4_streaming/24_kafka_stream_integration.ipynb) | Kafka binary serializer/deserializer, `foreachBatch` custom database sinks |
| **25** | Checkpointing & Recovery | [`25_checkpointing_fault_tolerance.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part4_streaming/25_checkpointing_fault_tolerance.py) | [`25_checkpointing_fault_tolerance.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part4_streaming/25_checkpointing_fault_tolerance.ipynb) | WAL offset/commit layout, simulated query crash & recovery verifying exactly-once |
| **26** | MLlib Feature Pipelines | [`26_mllib_pipelines_transformers.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part5_ml_analytics/26_mllib_pipelines_transformers.py) | [`26_mllib_pipelines_transformers.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part5_ml_analytics/26_mllib_pipelines_transformers.ipynb) | Transformers, Estimators, StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler |
| **27** | Churn Classification | [`27_churn_classification_tuning.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part5_ml_analytics/27_churn_classification_tuning.py) | [`27_churn_classification_tuning.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part5_ml_analytics/27_churn_classification_tuning.ipynb) | LogisticRegression, 3-fold CrossValidator, ParamGridBuilder, model save & load |
| **28** | Clustering & ALS | [`28_clustering_kmeans_als.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part5_ml_analytics/28_clustering_kmeans_als.py) | [`28_clustering_kmeans_als.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part5_ml_analytics/28_clustering_kmeans_als.ipynb) | KMeans customer RFM segmentation (Silhouette score), ALS product recommendation |
| **29** | NLP Text Processing | [`29_nlp_text_processing_reviews.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part5_ml_analytics/29_nlp_text_processing_reviews.py) | [`29_nlp_text_processing_reviews.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part5_ml_analytics/29_nlp_text_processing_reviews.ipynb) | RegexTokenizer, StopWordsRemover, HashingTF, IDF, Word2Vec embeddings |
| **30** | Graph Processing | [`30_graph_processing_graphframes.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part5_ml_analytics/30_graph_processing_graphframes.py) | [`30_graph_processing_graphframes.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part5_ml_analytics/30_graph_processing_graphframes.ipynb) | Vertices & Edges, in/out-degree, motif co-purchase triangles, PageRank concept |
| **31** | pandas API on Spark | [`31_pandas_api_on_spark.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part6_modern_ecosystem/31_pandas_api_on_spark.py) | [`31_pandas_api_on_spark.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part6_modern_ecosystem/31_pandas_api_on_spark.ipynb) | Migrating pandas code, distributed index types, interoperability |
| **32** | Spark Connect | [`32_spark_connect_client.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part6_modern_ecosystem/32_spark_connect_client.py) | [`32_spark_connect_client.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part6_modern_ecosystem/32_spark_connect_client.ipynb) | Decoupled client-server architecture over gRPC protocol |
| **33** | Delta Lake ACID | [`33_table_formats_delta_lake.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part6_modern_ecosystem/33_table_formats_delta_lake.py) | [`33_table_formats_delta_lake.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part6_modern_ecosystem/33_table_formats_delta_lake.ipynb) | ACID transactions, Time Travel (`versionAsOf`), atomic `MERGE` upserts |
| **34** | Declarative Pipelines | [`34_spark_declarative_pipelines.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part6_modern_ecosystem/34_spark_declarative_pipelines.py) | [`34_spark_declarative_pipelines.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part6_modern_ecosystem/34_spark_declarative_pipelines.ipynb) | Declarative table definitions, Data Quality Expectations (`DROP`, `FAIL`, `WARN`) |
| **35** | Python Data Source API | [`35_python_custom_data_source.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part6_modern_ecosystem/35_python_custom_data_source.py) | [`35_python_custom_data_source.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part6_modern_ecosystem/35_python_custom_data_source.ipynb) | Pure Python custom connectors (`DataSource`, `DataSourceReader`, `InputPartition`) |
| **36** | Catalog & Metastore | [`36_catalog_metastore_tables.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part6_modern_ecosystem/36_catalog_metastore_tables.py) | [`36_catalog_metastore_tables.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part6_modern_ecosystem/36_catalog_metastore_tables.ipynb) | Managed vs External tables, DROP safety differences, namespace management |
| **37** | Deployment & spark-submit | [`37_deployment_modes_spark_submit.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part7_production_capstone/37_deployment_modes_spark_submit.py) | [`37_deployment_modes_spark_submit.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part7_production_capstone/37_deployment_modes_spark_submit.ipynb) | Client vs Cluster deploy modes, sizing rules, production K8s & Standalone submit |
| **38** | Testing with Pytest | [`38_testing_spark_pytest.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part7_production_capstone/38_testing_spark_pytest.py) | [`38_testing_spark_pytest.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part7_production_capstone/38_testing_spark_pytest.ipynb) | Session-scoped fixtures, pure function testing, schema & value assertions |
| **39** | Monitoring & History Server | [`39_monitoring_history_server.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part7_production_capstone/39_monitoring_history_server.py) | [`39_monitoring_history_server.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part7_production_capstone/39_monitoring_history_server.ipynb) | Event logs (`spark.eventLog.enabled`), Log4j2 configuration, History Server (:18080) |
| **40** | Security & Anti-Patterns | [`40_security_best_practices.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part7_production_capstone/40_security_best_practices.py) | [`40_security_best_practices.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part7_production_capstone/40_security_best_practices.ipynb) | SHA-256 PII masking, secrets via Vault/Environment, Top 5 Anti-patterns |
| **41** | **Capstone Mini-Project** | [`41_capstone_medallion_pipeline.py`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/scripts/part7_production_capstone/41_capstone_medallion_pipeline.py) | [`41_capstone_medallion_pipeline.ipynb`](file:///home/umesh-pc/Desktop/workspace/apache-spark-example-worflow/notebooks/part7_production_capstone/41_capstone_medallion_pipeline.ipynb) | **End-to-End Medallion Lakehouse (Bronze -> Silver -> Gold + Streaming + ML Churn Model + Exec Report)** |

---

## 🛠️ Testing Verification
Run the complete automated test suite verifying all 41 modules:
```bash
make test
```
Result: **22 passed in ~11s**.

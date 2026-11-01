/*
 * Module 06: Apache Spark Dataset API (Scala / Typed Operations)
 *
 * 1. What It Is:
 * The Dataset API is a strongly typed, object-oriented programming interface available in Scala and Java.
 * It combines the type-safety and lambda expressions of RDDs with the execution speed, schema awareness,
 * and Tungsten optimizations of DataFrames. (In fact, in Spark 2.x+, DataFrame is simply a type alias for Dataset[Row]).
 *
 * 2. Why It Matters:
 * In languages like Scala and Java, compile-time type safety prevents runtime attribute typos (e.g. `customer.firts_name`
 * fails during compilation, not hours into a production cluster job).
 * Note: The Dataset API is NOT available in Python because Python is a dynamically typed language without JVM compile-time checks.
 *
 * 3. Architecture Comparison:
 * +----------------------+-----------------------+--------------------------+
 * | Feature              | RDD                   | DataFrame (Dataset[Row]) | Dataset[T] (Typed)       |
 * +----------------------+-----------------------+--------------------------+
 * | Typing               | Strongly typed JVM    | Untyped (Generic Rows)   | Strongly typed JVM       |
 * | Optimization         | None (developer-led)  | Catalyst & Tungsten      | Catalyst & Tungsten      |
 * | Compile Safety       | Compile-time          | Runtime Analysis         | Compile-time             |
 * | Language Support     | Scala, Java, Python   | Scala, Java, Python, R   | Scala, Java ONLY         |
 * +----------------------+-----------------------+--------------------------+
 *
 * How to Run:
 *   spark-shell -i scripts/part1_foundations/06_dataset_api_scala.scala
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// 1. Define Strongly-Typed Case Classes (Domain Models)
case class Customer(
  customer_id: Long,
  first_name: String,
  last_name: String,
  email: String,
  city: String,
  state: String,
  signup_date: String,
  loyalty_tier: String
)

case class HighValueCustomer(
  customer_id: Long,
  full_name: String,
  tier: String
)

// 2. Initialize or Use Existing SparkSession
val spark = SparkSession.builder()
  .appName("Module06-DatasetAPI-Scala")
  .getOrCreate()

import spark.implicits._

println("=================================================================")
println(" Module 06: Strongly-Typed Dataset API in Scala                  ")
println("=================================================================")

// 3. Load Untyped DataFrame and Cast to Typed Dataset[Customer]
val rawDf = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("data/raw/customers.csv")

// .as[Customer] binds the generic Catalyst schema to the JVM case class
val customerDs = rawDf.as[Customer]

println(s"[✓] Successfully loaded Dataset[Customer] with count: ${customerDs.count()}")
customerDs.show(3)

// 4. Strongly-Typed Transformations with Compile-Time Check
// Notice: 'c.loyalty_tier' is checked at compile-time by the Scala compiler!
val platinumDs = customerDs
  .filter(c => c.loyalty_tier == "Platinum")
  .map(c => HighValueCustomer(c.customer_id, s"${c.first_name} ${c.last_name}", c.loyalty_tier))

println("[✓] Strongly-Typed Mapped Dataset[HighValueCustomer]:")
platinumDs.show(5)

// 5. Interoperability: Transitioning between Dataset[T] and DataFrame (Dataset[Row])
val standardDf = platinumDs.toDF()
println(s"[✓] Converted back to untyped DataFrame (Dataset[Row]): Schema is ${standardDf.schema.simpleString}")

println("[*] Module 06 (Scala) completed successfully.")

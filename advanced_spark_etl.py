from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, row_number, avg, broadcast
from pyspark.sql.window import Window

# Initialize SparkSession for a cluster
spark = SparkSession.builder \
    .appName("AdvancedSparkETL") \
    .master("spark://<MASTER_NODE_IP>:7077") \  # Replace with cluster master IP
    .config("spark.sql.shuffle.partitions", "50") \  # Optimize shuffle partitions
    .config("spark.sql.execution.arrow.enabled", "true") \  # Enable Arrow for Pandas UDFs (faster computation)
    .getOrCreate()

# 🚀 **1. Generate Large Sample Data in JSON Format**
json_data = """
[
    {"user_id": 1, "orders": [{"order_id": "A1", "amount": 100}, {"order_id": "A2", "amount": 150}]},
    {"user_id": 2, "orders": [{"order_id": "B1", "amount": 200}, {"order_id": "B2", "amount": 50}, {"order_id": "B3", "amount": 300}]}
]
"""

# 🚀 **2. Read JSON Data Using Spark's Parallelized RDDs**
df = spark.read.json(spark.sparkContext.parallelize([json_data]))

# 🚀 **3. Explode the 'orders' array (Parallel Processing)**
exploded_df = df.select("user_id", explode("orders").alias("order"))

# 🚀 **4. Flatten the nested 'order' struct**
flattened_df = exploded_df.select("user_id", "order.order_id", "order.amount")

# 🚀 **5. Partition the DataFrame (Optimize Parallel Execution)**
flattened_df = flattened_df.repartition("user_id")  # Distribute load evenly

# 🚀 **6. Use Window Functions for Aggregations**
window_spec = Window.partitionBy("user_id")

# Compute Average Order Amount per User (Distributed Computation)
result_df = flattened_df.withColumn("avg_order_amount", avg("amount").over(window_spec))

# Rank orders by amount per user (Efficient Ranking)
window_rank = Window.partitionBy("user_id").orderBy(col("amount").desc())
result_df = result_df.withColumn("rank", row_number().over(window_rank))

# 🚀 **7. Broadcast Join for Fast Small-Large Joins**
# Simulated small lookup dataset (e.g., User Metadata)
user_data = """
[
    {"user_id": 1, "country": "Netherlands"},
    {"user_id": 2, "country": "Germany"}
]
"""

# Read JSON as DataFrame
user_df = spark.read.json(spark.sparkContext.parallelize([user_data]))

# Use `broadcast()` for small dataset joins (Avoid expensive shuffles)
joined_df = result_df.join(broadcast(user_df), "user_id", "left")

# 🚀 **8. Cache Frequently Used Data (Speed Up Iterative Queries)**
joined_df.cache()

# 🚀 **9. Write to Parquet (Columnar Storage for Fast Queries)**
joined_df.write.mode("overwrite").partitionBy("country").parquet("output/optimized_orders")

# 🚀 **10. Stop SparkSession**
spark.stop()

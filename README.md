# 🚀 Advanced PySpark ETL Project

This repository contains an **optimized PySpark ETL pipeline** that demonstrates **key features of Apache Spark** for **big data processing**. The ETL efficiently loads, processes, and stores data using advanced Spark techniques.

## 📌 Features
- **Uses `spark.read.json()` to load JSON directly from an RDD** (no need for files!).
- **Leverages Spark's distributed computing** for **scalability & performance**.
- **Uses window functions for efficient aggregations & ranking**.
- **Partitions data dynamically** for **optimized parallel execution**.
- **Broadcast joins for small-large table joins** (faster than standard joins).
- **Caches DataFrame for repeated queries** to **reduce execution time**.
- **Saves processed data in Parquet format** with **partitioning by country**.

## 🛠 Prerequisites
Before running this project, ensure you have:
- **Apache Spark installed** (Standalone, YARN, or Kubernetes cluster).
- **Python 3.x**.
- **PySpark installed**:
  ```sh
  pip install pyspark
  ```

## 🚀 How to Run
1. **Start your Spark cluster** (if running in cluster mode):
   ```sh
   start-master.sh  # Start Spark master
   start-worker.sh spark://<MASTER_NODE_IP>:7077  # Start worker nodes
   ```

2. **Run the ETL script using `spark-submit`**:
   ```sh
   spark-submit --master spark://<MASTER_NODE_IP>:7077 etl_script.py
   ```
   Replace `<MASTER_NODE_IP>` with your Spark master node's IP address.

3. **Monitor the job in the Spark UI**:
   Open **http://<MASTER_NODE_IP>:8080** in a browser.

## 📂 Project Structure
```
/
│── etl_script.py      # Main ETL pipeline
│── output/            # Processed data (Parquet format)
│── README.md          # Project documentation
```

## ⚡ Expected Output
- **Parquet files partitioned by `country`** in `output/optimized_orders`.
- **Optimized ETL execution using partitioning, caching, and broadcast joins**.
- **Faster processing than traditional Python/Pandas scripts**.

## 🎯 Why Use This ETL?
✅ **Handles Big Data Efficiently** – Works on **distributed clusters**.
✅ **Faster Than Pandas** – Uses **parallel execution & lazy evaluation**.
✅ **Scalable** – Can **process terabytes of data without memory issues**.
✅ **Optimized for Queries** – Saves data in **Parquet with partitioning**.

## 💡 Future Enhancements
- Implement **Spark UDFs** for custom Python functions.
- Enable **Dynamic Partition Pruning** for faster queries.
- Integrate with **AWS S3 or HDFS** for cloud storage.

---


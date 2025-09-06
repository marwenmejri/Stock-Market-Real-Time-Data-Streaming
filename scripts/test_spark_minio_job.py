from pyspark.sql import SparkSession
import os

# Create SparkSession with event logging enabled
spark = SparkSession.builder \
    .appName("TestSparkMinioJob") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:///opt/bitnami/spark/history") \
    .getOrCreate()

# Dummy DataFrame
data = [("Marwen", 1), ("Mejri", 2)]
df = spark.createDataFrame(data, ["name", "id"])

# Write to MinIO
df.write.mode("overwrite").parquet("s3a://stock-data/test-output/")

print("✅ Job completed successfully.")

spark.stop()
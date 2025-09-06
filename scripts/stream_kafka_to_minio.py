from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import from_json, col, to_timestamp

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToMinioStreamingEnhanced") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", LongType(), True),
    StructField("currency", StringType(), True),
    StructField("exchange", StringType(), True)
])

# Exchange region metadata
exchange_metadata = spark.createDataFrame([
    ("NASDAQ", "USA"),
    ("NYSE", "USA"),
    ("TSE", "Japan")
], ["exchange", "region"])

# Step 1: Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094") \
    .option("subscribe", "stock-market") \
    .option("startingOffsets", "latest") \
    .load()

# Step 2: Extract and parse JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING) as json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp")))

# 👇 Console preview of parsed data
parsed_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

# Step 3: Filter
filtered_df = parsed_df.filter(
    (col("symbol").isin("AAPL", "GOOGL", "MSFT")) &
    (col("close") > 0)
)

# 👇 Console preview of filtered data
filtered_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

# Step 4: Enrich
enriched_df = filtered_df \
    .withColumn("price_range", col("high") - col("low")) \
    .join(exchange_metadata, on="exchange", how="left")

# 👇 Console preview of enriched data
enriched_df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .outputMode("append") \
    .trigger(processingTime="30 seconds") \
    .start()

# Step 5: Write to MinIO in Parquet format
enriched_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/tmp/spark-checkpoint/stock-market-enhanced") \
    .option("path", "s3a://realtime-data/stock-market-enhanced/") \
    .outputMode("append") \
    .trigger(processingTime="15 seconds") \
    .start() \
    .awaitTermination()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
from pyspark.sql.functions import from_json, col, to_timestamp

# -------------------------------------------------------------------
# 1. Spark session
# -------------------------------------------------------------------
spark = SparkSession.builder \
    .appName("KafkaToMinioStreamingEnhanced") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------------------
# 2. Schema
# -------------------------------------------------------------------
schema = StructType([
    StructField("symbol", StringType()),
    StructField("timestamp", StringType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", LongType()),
    StructField("currency", StringType()),
    StructField("exchange", StringType())
])

# -------------------------------------------------------------------
# 3. Metadata
# -------------------------------------------------------------------
exchange_metadata = spark.createDataFrame([
    ("NASDAQ", "USA"),
    ("NYSE", "USA"),
    ("TSE", "Japan"),
    ("HKEX", "Hong Kong")
], ["exchange", "region"])

# -------------------------------------------------------------------
# 4. Read from Kafka
# -------------------------------------------------------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9093,kafka3:9094") \
    .option("subscribe", "stock-market") \
    .option("startingOffsets", "latest") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json")
parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
parsed_df = parsed_df.withColumn("event_time", to_timestamp(col("timestamp")))

# -------------------------------------------------------------------
# 5. Transformations
# -------------------------------------------------------------------
filtered_df = parsed_df.filter(
    (col("symbol").isin("AAPL", "GOOGL", "MSFT", "TSLA")) &
    (col("close") > 0)
)

enriched_df = filtered_df.withColumn("price_range", col("high") - col("low")) \
    .join(exchange_metadata, on="exchange", how="left")

# -------------------------------------------------------------------
# 6. Write to MinIO (Parquet)
# -------------------------------------------------------------------
query = enriched_df.writeStream \
    .format("parquet") \
    .option("path", "s3a://realtime-data/stock-market-enhanced/") \
    .option("checkpointLocation", "/tmp/spark-checkpoint/stock-market-enhanced") \
    .outputMode("append") \
    .trigger(processingTime="15 seconds") \
    .start()

query.awaitTermination()

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TestSparkHistory") \
        .getOrCreate()

    # Dummy data
    lines = spark.sparkContext.parallelize([
        "Apache Spark is fast",
        "Apache Spark is flexible",
        "Apache Spark is powerful"
    ])

    word_counts = (
        lines.flatMap(lambda line: line.split(" "))
             .map(lambda word: (word, 1))
             .reduceByKey(lambda a, b: a + b)
    )

    # Show and save
    word_counts.foreach(print)
    word_counts.saveAsTextFile("file:///opt/bitnami/spark/output_test")

    spark.stop()

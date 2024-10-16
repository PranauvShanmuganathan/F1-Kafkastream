from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, from_json
from jobs.config import configuration


def main():
    # Step 1: Initialize Spark Session
    spark = SparkSession.builder.appName("SmartCityStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.11.469,"
                "org.scala-lang:scala-library:2.13.12") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .config("spark.sql.streaming.checkpointLocation", "s3://formla1-kafkasave/Checkpoint/") \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # Step 2: Define the Schema for the Kafka Messages
    schema = StructType([
        StructField("driverId", StringType()),
        StructField("permanentNumber", IntegerType()),
        StructField("code", StringType()),
        StructField("url", StringType()),
        StructField("givenName", StringType()),
        StructField("familyName", StringType()),
        StructField("dateOfBirth", StringType()),
        StructField("nationality", StringType())
    ])

    # Step 3: Read Data from Kafka Topic
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "kafka_stream") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Step 5: Show the Consumed Data
    # Write the data to the console for visibility
    query = kafka_df.writeStream \
        .format('parquet') \
        .option('checkpointLocation', 's3://formla1-kafkasave/Checkpoint/') \
        .option('path', 's3://formla1-kafkasave/output/') \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

    # Wait for the termination of the stream (so Spark keeps running)
    query.awaitTermination()


if __name__ == "__main__":
    main()

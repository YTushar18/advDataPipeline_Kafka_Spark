import json
import requests
import config

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Create a Spark session with the Kafka package
spark = SparkSession.builder \
    .appName("MarketDataProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Define the schema for the incoming JSON data
schema = StructType([
    StructField("01. symbol", StringType(), True),
    StructField("05. price", StringType(), True),
    # Add more fields based on your data structure
])

# Kafka configuration for Spark
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "marketdata"
}

# Read data from Kafka
raw_stream = spark.readStream.format("kafka").options(**kafka_params).load()

# Parse JSON data
json_stream = raw_stream.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("data"))

# Perform further processing as needed
processed_stream = json_stream.select("data.*")

# Define the query sink and checkpoint location
checkpoint_dir = "/Users/dhrutipatel/AdvancedDB/advDataPipeline_Kafka_Spark/checkpoint"  # Specify your checkpoint directory
output_mode = "append"  # Change to "complete" or "update" if needed
output_sink = "console"  # Change to "parquet", "jdbc", etc., for other sinks

# Output the processed data (you can modify this based on your requirements)
query = processed_stream.writeStream \
    .outputMode(output_mode) \
    .option("checkpointLocation", checkpoint_dir) \
    .format(output_sink) \
    .start()

# Await termination of the query
try:
    query.awaitTermination()
except KeyboardInterrupt:
    # Stop the streaming query gracefully
    query.stop()

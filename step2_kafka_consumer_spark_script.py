import json
import requests
import config

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.functions import col, lit, explode
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.types import StructType, StructField, StringType, MapType, TimestampType

from pyspark.sql import SparkSession



# Create a Spark session with the Kafka package
spark = SparkSession.builder \
    .appName("MarketDataProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "marketdata"
}

value_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("timeseries", StringType(), True),
    StructField("open", StringType(), True),
    StructField("high", StringType(), True),
    StructField("low", StringType(), True),
    StructField("close", StringType(), True),
    StructField("volume", StringType(), True),
])



# Read data from Kafka topic
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "marketdata") \
    .load()


kafka_df = kafka_df.select(from_json(col("value").cast("string"), value_schema) \
                        .alias("values"))

from pyspark.sql.functions import explode


parsed_df = kafka_df.select("values.*")

# qry = parsed_df.writeStream.outputMode("append").format("console").start()

# qry.awaitTermination()

# Write the streaming DataFrame to an in-memory table
query = parsed_df \
    .writeStream \
    .format("memory") \
    .queryName("my_table8") \
    .start()

# Wait for the streaming query to start
query.awaitTermination(10)

# Query the in-memory table using Spark SQL to create a static DataFrame
static_df = spark.sql("SELECT * FROM my_table8")

# Show the resulting static DataFrame
# static_df.show(truncate=False)
static_df.printSchema()

from pyspark.sql.functions import split, expr
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Modify the DataFrame to include "open," "high," "low," "close," and "volume" columns
df = static_df.withColumn("time", expr("split(trim('[]', timeseries), ',' )")) \
    .withColumn("open", expr("split(trim('[]', open), ',' )")) \
    .withColumn("high", expr("split(trim('[]', high), ',' )")) \
    .withColumn("low", expr("split(trim('[]', low), ',' )")) \
    .withColumn("close", expr("split(trim('[]', close), ',' )")) \
    .withColumn("volume", expr("split(trim('[]', volume), ',' )"))

# Use arrays_zip with the modified columns
df = df.withColumn("new", expr("arrays_zip(time, open, high, low, close, volume)")) \
       .withColumn("new", explode("new")) \
       .select("symbol", col("new.time").alias("timeseries"), col("new.open").alias("open"),
               col("new.high").alias("high"), col("new.low").alias("low"),
               col("new.close").alias("close"), col("new.volume").alias("volume"))

df.show(truncate=False)





import pandas as pd
import matplotlib.pyplot as plt


print(df)

# Convert Spark DataFrame to Pandas DataFrame for plotting
pandas_df = df.toPandas()

# Ensure 'timeseries' column is of string type
pandas_df['timeseries'] = pandas_df['timeseries'].astype(str)

# Remove double quotes around the timestamp string
pandas_df['timeseries'] = pandas_df['timeseries'].str.strip('"')

# Convert 'date' column to datetime type
pandas_df['date'] = pd.to_datetime(pandas_df['timeseries'], format='%Y-%m-%d %H:%M:%S')

# Convert '4. close' to a numeric type
pandas_df['close'] = pandas_df['close'].str.strip('"')
pandas_df['close'] = pandas_df['close'].astype(float)

# Sort the DataFrame by date
pandas_df.sort_values('date', inplace=True)

# Setting up the figure for the dashboard
fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(14, 10))
fig.suptitle("MSFT Stock Analysis Dashboard", fontsize=16)

# Plot 1: Closing Prices
axes[0].plot(pandas_df['date'].values, pandas_df['close'].values, label='Closing Price', color='blue', alpha=0.7)
axes[0].set_title('Closing Prices Over Time')
axes[0].set_xlabel('Date')
axes[0].set_ylabel('Closing Price')
axes[0].legend()

# Calculate the moving averages
pandas_df['7_day_SMA'] = pandas_df['close'].rolling(window=7).mean()
pandas_df['30_day_SMA'] = pandas_df['close'].rolling(window=30).mean()

# Plot 2: Moving Averages
axes[1].plot(pandas_df['date'].values, pandas_df['close'].values, label='Closing Price', color='blue', alpha=0.5)
axes[1].plot(pandas_df['date'].values, pandas_df['7_day_SMA'].values, label='7-Day SMA', color='red')
axes[1].plot(pandas_df['date'].values, pandas_df['30_day_SMA'].values, label='30-Day SMA', color='green')
axes[1].set_title('Closing Prices with 7-Day and 30-Day Moving Averages')
axes[1].set_xlabel('Date')
axes[1].set_ylabel('Price')
axes[1].legend()

# Adjust layout
plt.tight_layout()
plt.subplots_adjust(top=0.9)

# Show the dashboard
plt.show()
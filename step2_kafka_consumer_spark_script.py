from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, window, avg, expr, lit, split
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, MapType, TimestampType
from pyspark.sql.types import *
from pyspark.sql.window import Window

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import dash
from dash import dcc
from dash import html

# Create a Spark session with the Kafka package
spark = SparkSession.builder \
    .appName("MarketDataProcessor") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "marketdata"
}

# Define the schema for the "value" field
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

parsed_df = kafka_df.select("values.*")

# Write the streaming DataFrame to an in-memory table
query = parsed_df \
    .writeStream \
    .format("memory") \
    .queryName("my_table") \
    .start()

# Wait for the streaming query to start
query.awaitTermination(10)

# Query the in-memory table using Spark SQL to create a static DataFrame
static_df = spark.sql("SELECT * FROM my_table")

# # Show the resulting static DataFrame
# # static_df.show(truncate=False)
static_df.printSchema()

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

# Convert Spark DataFrame to Pandas DataFrame for plotting
import pandas as pd
pandas_df = df.toPandas()

# Clean up the 'timeseries' column
pandas_df['timeseries'] = pandas_df['timeseries'].str.strip('"')
# Convert 'timeseries' to datetime type with format 'yyyy-MM-dd HH:mm:ss'
pandas_df['timeseries'] = pd.to_datetime(pandas_df['timeseries'], format='%Y-%m-%d %H:%M:%S')

# Clean up and convert numeric columns
numeric_columns = ['open', 'high', 'low', 'close', 'volume']
for column in numeric_columns:
    pandas_df[column] = pd.to_numeric(pandas_df[column].str.strip('"'))

# Calculate Relative Strength Index (RSI)
rsi_window = 14  # Specify the window size for RSI
delta = pandas_df['close'].diff(1)
gain = delta.where(delta > 0, 0)
loss = -delta.where(delta < 0, 0)

avg_gain = gain.rolling(window=rsi_window).mean()
avg_loss = loss.rolling(window=rsi_window).mean()

rs = avg_gain / avg_loss
rsi = 100 - (100 / (1 + rs))

pandas_df['rsi'] = rsi
# Create a Spark DataFrame from the cleaned Pandas DataFrame
spark_df = spark.createDataFrame(pandas_df)

# Plotting with Plotly

# Plot Closing Prices Over Time
fig_closing_prices = px.line(pandas_df, x='timeseries', y='close', title='Closing Prices Over Time')
# fig_closing_prices.show()

# Candlestick Chart
fig_candlestick = go.Figure(data=[go.Candlestick(x=pandas_df['timeseries'],
                                                 open=pandas_df['open'],
                                                 high=pandas_df['high'],
                                                 low=pandas_df['low'],
                                                 close=pandas_df['close'])])
fig_candlestick.update_layout(title='Candlestick Chart - Open, High, Low, Close Over Time',
                              xaxis_title='Time',
                              yaxis_title='Price',
                              xaxis_rangeslider_visible=False)
# fig_candlestick.show()

# Stock Price Over Time with Moving Averages
fig_stock_price = px.line(pandas_df, x='timeseries', y=['open', 'low', 'close'],
                          labels={'value': 'Stock Price', 'timeseries': 'Time'},
                          title='Stock Price Over Time with Moving Averages')
# fig_stock_price.show()

# Extract relevant columns for volume analysis
volume_df = pandas_df[['timeseries', 'volume']]

# Plot Trading Volume Over Time
fig_volume = px.bar(volume_df, x='timeseries', y='volume', title='Trading Volume Over Time')
# fig_volume.show()

# Plot Closing Prices Over Time with RSI
fig_closing_prices_rsi = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                       subplot_titles=['Closing Prices Over Time', 'RSI Over Time'])

fig_closing_prices_rsi.add_trace(go.Scatter(x=pandas_df['timeseries'], y=pandas_df['close'],
                                           mode='lines', name='Closing Prices'), row=1, col=1)

fig_closing_prices_rsi.add_trace(go.Scatter(x=pandas_df['timeseries'], y=pandas_df['rsi'],
                                           mode='lines', name='RSI', yaxis='y2'), row=2, col=1)

fig_closing_prices_rsi.update_layout(title_text='Closing Prices Over Time with RSI',
                                     xaxis_title='Time',
                                     yaxis_title='Closing Prices',
                                     yaxis2_title='RSI',
                                     xaxis_rangeslider_visible=False)

# fig_closing_prices_rsi.show()
# Initialize the Dash application
app = dash.Dash(__name__)

# Define the layout of the dashboard
app.layout = html.Div([
    html.H1("Financial Market Data Dashboard", 
            style={'textAlign': 'center', 'margin-top': '20px', 'color': '#6b74fc', 'font-family': 'Arial'}),

    html.Div([
        html.H3("Closing Prices Over Time", style={'color': '#333366'}),
        html.P("This graph shows the trend of closing prices over a specified time period, "
               "allowing for the observation of general price movements and patterns.",
               style={'fontWeight': '500', 'color': '#4a4a4a'}),
        dcc.Graph(figure=fig_closing_prices)
    ], style={'margin-bottom': '40px', 'padding': '20px', 'border': '1px solid #ddd'}),

    html.Div([
        html.H3("Candlestick Chart", style={'color': '#333366'}),
        html.P("The candlestick chart provides a detailed representation of price movements, "
               "including open, high, low, and close values, which are crucial for technical analysis.",
               style={'fontWeight': '500', 'color': '#4a4a4a'}),
        dcc.Graph(figure=fig_candlestick)
    ], style={'margin-bottom': '40px', 'padding': '20px', 'border': '1px solid #ddd'}),

    html.Div([
        html.H3("Stock Price with Moving Averages", style={'color': '#333366'}),
        html.P("This line chart displays stock prices along with moving averages, "
               "highlighting trends and potential future movements.",
               style={'fontWeight': '500', 'color': '#4a4a4a'}),
        dcc.Graph(figure=fig_stock_price)
    ], style={'margin-bottom': '40px', 'padding': '20px', 'border': '1px solid #ddd'}),

    html.Div([
        html.H3("Trading Volume Over Time", style={'color': '#333366'}),
        html.P("The bar chart of trading volume over time helps in understanding the activity level "
               "and liquidity of the stock, which is important for assessing market sentiment.",
               style={'fontWeight': '500', 'color': '#4a4a4a'}),
        dcc.Graph(figure=fig_volume)
    ], style={'margin-bottom': '40px', 'padding': '20px', 'border': '1px solid #ddd'}),

    html.Div([
        html.H3("Closing Prices with RSI", style={'color': '#333366'}),
        html.P("This combined graph shows closing prices and the Relative Strength Index (RSI), "
               "a momentum indicator used to identify overbought or oversold conditions. RSI values range from 0 to 100, with readings above 70 indicating overbought conditions and readings below 30 indicating oversold conditions.",
               style={'fontWeight': '500', 'color': '#4a4a4a'}),
        dcc.Graph(figure=fig_closing_prices_rsi)
    ], style={'margin-bottom': '40px', 'padding': '20px', 'border': '1px solid #ddd'})
], style={'font-family': 'Arial', 'padding': '20px'})

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
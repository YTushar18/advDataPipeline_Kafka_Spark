# from alpha_vantage.timeseries import TimeSeries
from confluent_kafka import Producer

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient, ContentSettings

import json
import pandas as pd
import time
import requests
import config
import logging

# Configure logging
# logging.basicConfig(filename='/Users/csuftitan/Documents/Development/Adv_DB_Project/marketDataAnalysis/data/python_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Function to fetch market data and produce to Kafka
def fetch_and_produce(symbol):

    # Intraday
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=5min&entitlement=delayed&apikey=Q2XTMKBHNQ9OJFND'

    # #Month
    # url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&month=2023-03&entitlement=delayed&apikey=Q2XTMKBHNQ9OJFND'

    r = requests.get(url)
    data = r.json()
    new = []

    newdict = {}

    for k in data["Time Series (5min)"].keys():
        new.append((data["Meta Data"]["2. Symbol"],k, data["Time Series (5min)"][k]["1. open"], data["Time Series (5min)"][k]["2. high"], data["Time Series (5min)"][k]["3. low"], data["Time Series (5min)"][k]["4. close"], data["Time Series (5min)"][k]["5. volume"]))

    df = pd.DataFrame(new)

    df.columns = ['Symbol', 'Timeseries', 'Open', 'High', 'Low', 'Close', 'Volume']

    newdict["symbol"] = df["Symbol"][0]
    newdict["timeseries"] = df['Timeseries'].to_list()
    newdict["open"] = df['Open'].to_list()
    newdict["high"] = df['High'].to_list()
    newdict["low"] = df['Low'].to_list()
    newdict["close"] = df['Close'].to_list()
    newdict["volume"] = df['Volume'].to_list()

    # Pushing to Kafka Topic

    # Produce data to Kafka
    producer = Producer(config.kafka_config)
    producer.produce(config.kafka_topic, key="MSFT", value=json.dumps(newdict))
    producer.flush()


    # Uploading to Cloud Storage

    # Create a BlobServiceClient
    blob_service_client = BlobServiceClient(account_url=f'https://{config.account_name}.blob.core.windows.net', credential=config.account_key)
    # Get a reference to the container
    container_client = blob_service_client.get_container_client(config.container_name)
    current_timestamp = time.strftime("%Y%m%d-%H%M%S")
    # Remote path (blob name) where the file will be stored in the Azure Blob Storage container
    remote_file_path = f'raw_market-data/{current_timestamp}-MSFT.json'
    # Convert the dictionary to a JSON-formatted string
    json_data = json.dumps(newdict)
    # Upload the JSON string to Azure Blob Storage
    container_client.upload_blob(name=remote_file_path, data=json_data, content_settings=ContentSettings(content_type='application/json'))
    
    # Example log messages
    # logging.info(f'executed at {current_timestamp}')

if __name__ == "__main__":
    
    data = fetch_and_produce(['MSFT'])

        




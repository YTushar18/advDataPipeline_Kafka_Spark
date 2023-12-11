import json
import requests
import logging
from confluent_kafka import Producer, KafkaError
import config
import pandas as pd
import time
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobClient, BlobServiceClient, ContentSettings

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def delivery_report(err, msg):
    if err:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def fetch_and_produce(symbol):
    try:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&entitlement=delayed&apikey={config.api_key}'
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()

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

            print(newdict)
            
            # Check if the expected data is present
            if "Time Series (5min)" in data:
                producer = Producer(config.kafka_config)
                producer.produce(
                    config.kafka_topic,
                    key=symbols,
                    value=json.dumps(newdict),
                    callback=delivery_report
                )
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
            else:
                logging.error(f"Unexpected JSON structure or error message: {data}")
        else:
            logging.error(f"HTTP Request failed with status code: {response.status_code}, response: {response.text}")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP Request Exception: {e}")
    except KafkaError as e:
        logging.error(f"Kafka Error: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"JSON Decode Error: {e}")

if __name__ == "__main__":
    # Extend this to loop over multiple symbols or read from a config/source
    # Right now, fecting the data of AAPL Only
    symbols = "AAPL"
    fetch_and_produce(symbols)       
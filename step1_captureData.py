import json
import requests
import logging
from confluent_kafka import Producer, KafkaError
import config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def delivery_report(err, msg):
    if err:
        logging.error('Message delivery failed: {}'.format(err))
    else:
        logging.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def fetch_and_produce(symbol):
    try:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={config.api_key}'
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            
            # Check if the expected data is present
            if "Time Series (5min)" in data:
                producer = Producer(config.kafka_config)
                producer.produce(
                    config.kafka_topic,
                    key=symbol.encode('utf-8'),
                    value=json.dumps(data).encode('utf-8'),
                    callback=delivery_report
                )
                producer.flush()
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
    symbols = config.list_of_stocks  # Example list of symbols
    for symbol in symbols:
        fetch_and_produce(symbol)

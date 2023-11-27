import requests
import json
import os
import logging
from datetime import datetime
import config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def capture_data(symbol):
    try:
        # Use the config module for the API key
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={config.api_key}'
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            # Validate data
            if "Time Series (Daily)" in data:
                # Define the file path with a timestamp
                filename = f'step1_TempDatadump/data_{symbol}_{datetime.now().strftime("%Y%m%d%H%M%S")}.json'
                os.makedirs(os.path.dirname(filename), exist_ok=True)
                with open(filename, 'w') as f:
                    json.dump(data, f)
                logging.info(f"Data for {symbol} captured successfully.")
                print(data)
            else:
                logging.error(f"Unexpected JSON structure: {data}")
        else:
            logging.error(f"HTTP Request failed with status code: {response.status_code}")
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Request exception: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")

# Example usage
capture_data('AAPL')
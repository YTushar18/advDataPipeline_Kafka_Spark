# Base URL of API
base_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&entitlement=delayed&apikey={config.api_key}'

# Alpha Vantage API Key
api_key = "" # Alpha Vantage API Key

list_of_stocks = ["IBM", "AAPL", "MSFT", "GOOGL", "NVDA", "META"]

# Kafka Configuration
kafka_config = {'bootstrap.servers': 'localhost:9092'}

# Kafka Topic
kafka_topic = 'marketdata'

# Azure Storage account credentials
account_name = 'cpsc531advdbproject'
account_key = '' # Account key

# Name of the container in Azure Blob Storage
container_name = 'market-historical-data'
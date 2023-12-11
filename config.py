
base_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo'

# Alpha Vantage API key
api_key = "Q2XTMKBHNQ9OJFND"

list_of_stocks = ["IBM", "AAPL", "MSFT", "GOOGL", "NVDA", "META"]


# Kafka configuration
kafka_config = {'bootstrap.servers': 'localhost:9092'}

# Kafka topic
kafka_topic = 'marketdata'


# Azure Storage account credentials
account_name = 'cpsc531advdbproject'
account_key = 'bn5E08o0+vC8sHyWUKUE0rxRzoitH4nA9pNQE9XMlG9uHTNn1t13GqsglOvN2xwAzQbgmrfabPfn+AStt3x4xQ=='

# Name of the container in Azure Blob Storage
container_name = 'market-historical-data'



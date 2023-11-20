
base_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo'

# Alpha Vantage API key
api_key = "PNVFVX2J7SFRHYDE"

list_of_stocks = ["IBM", "AAPL", "MSFT", "GOOGL", "NVDA", "META"]


# Kafka configuration
kafka_config = {'bootstrap.servers': 'localhost:9092'}

# Kafka topic
kafka_topic = 'marketdata'


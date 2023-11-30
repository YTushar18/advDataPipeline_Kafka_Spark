# from alpha_vantage.timeseries import TimeSeries
from confluent_kafka import Producer
import json
import requests
import config

# Function to fetch market data and produce to Kafka
def fetch_and_produce(list_of_stocks):
    # ts = TimeSeries(key=config.api_key, output_format='json')
    # data, meta_data = ts.get_quote_endpoint(symbol=symbol)

    marketData = []

    for symbol in list_of_stocks:
        url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval=5min&apikey={config.api_key}'
        r = requests.get(url)
        data = r.json()

        print(data)

        # marketData.append(data)
        # key = data["Meta Data"]["2. Symbol"]

        # marketData.update({key:data})
        with open("step1_TempDataDump/data.json", "w") as outfile:
            json.dump(marketData,outfile)




    # print(data)

    # Produce data to Kafka
    producer = Producer(config.kafka_config)
    producer.produce(config.kafka_topic, key=symbol, value=json.dumps(data))
    producer.flush()

    # print("hi")

if __name__ == "__main__":
    # Example usage
    
    data = fetch_and_produce(['META'])

        




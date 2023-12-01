from confluent_kafka import Consumer, KafkaException
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
    'group.id': 'my_market_data_processor',  # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start reading from the beginning of the topic if no offset is stored
}

# Kafka topic to consume from
kafka_topic = 'marketdata'

def process_message(msg_value):
    # Process the message (e.g., transform it, store it, etc.)
    logging.info(f"Received message: {msg_value}")

def main():
    # Create a Kafka consumer
    consumer = Consumer(kafka_config)
    
    # Subscribe to the Kafka topic
    consumer.subscribe([kafka_topic])
    
    try:
        while True:
            # Poll for messages
            msg = consumer.poll(timeout=1.0)  # Adjust the timeout as needed
    
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event, not an error
                    continue
                else:
                    logging.error(msg.error())
                    break
    
            # Decode the message value and process the message
            try:
                message_value = json.loads(msg.value().decode('utf-8'))
                print(message_value)
                # process_message(message_value)
            except json.JSONDecodeError as e:
                logging.error(f"JSON Decode Error: {e}")
    
            # Commit the message offset if message processed successfully
            consumer.commit(asynchronous=False)
    
    except KeyboardInterrupt:
        logging.info("Shutdown signal received.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == "__main__":
    main()

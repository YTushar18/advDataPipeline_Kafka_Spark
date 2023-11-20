from confluent_kafka import Consumer, KafkaException
import json

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker(s)
    'group.id': 'my_market_data_processor',       # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start reading from the beginning of the topic if no offset is stored
}

# Kafka topic to consume from
kafka_topic = 'marketdata'

# Create a Kafka consumer
consumer = Consumer(kafka_config)

# Subscribe to the Kafka topic
consumer.subscribe([kafka_topic])

try:
    while True:
        # Poll for messages
        msg = consumer.poll(timeout=1000)  # Adjust the timeout as needed

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaException._PARTITION_EOF:
                # End of partition event, not an error
                continue
            else:
                print(msg.error())
                break

        # Decode and print the message value
        message_value = json.loads(msg.value().decode('utf-8'))
        print(f"Received message: {message_value}")

except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    consumer.close()

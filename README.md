# Adv Database Project

# Team Members
  1. Tushar Yadav
  2. Dhruti Dilipbhai Patel

# Setting up the environment

1. Install latest Java on your mac 
2. Install Apache Kafka (https://medium.com/@Ankitthakur/apache-kafka-installation-on-mac-using-homebrew-a367cdefd273)
3. Install Apache Spark (https://sparkbyexamples.com/spark/install-apache-spark-on-mac/)
4. Install python 3.7 or later
5. Install Dependencies using pip - python3 -m pip install -r requirements.txt

# Starting kafka server

START THE KAFKA ENVIRONMENT
NOTE: Your local environment must have Java 8+ installed.

Apache Kafka can be started using ZooKeeper or KRaft. To get started with either configuration follow one the sections below but not both.

Kafka with ZooKeeper
Run the following commands in order to start all services in the correct order:

# Start the ZooKeeper service
$ bin/zookeeper-server-start.sh config/zookeeper.properties
Open another terminal session and run:

# Start the Kafka broker service
$ bin/kafka-server-start.sh config/server.properties
Once all services have successfully launched, you will have a basic Kafka environment running and ready to use.

# CREATE A TOPIC TO STORE YOUR EVENTS
Kafka is a distributed event streaming platform that lets you read, write, store, and process events (also called records or messages in the documentation) across many machines.

Example events are payment transactions, geolocation updates from mobile phones, shipping orders, sensor measurements from IoT devices or medical equipment, and much more. These events are organized and stored in topics. Very simplified, a topic is similar to a folder in a filesystem, and the events are the files in that folder.

So before you can write your first events, you must create a topic. Open another terminal session and run:
$ bin/kafka-topics.sh --create --topic marketdata --bootstrap-server localhost:9092

# LIST KAFKA TOPICS

bin/kafka-topics.sh --bootstrap-server=localhost:9092 --list

# Run Python Scripts

1. open 2 seperate new terminals
2. Run step1_captureData.py in one ternimals to capture data from API and send it to kafka topic
3. Run step2_kafka_consumer_spark_script.py to get data from Kafka into spark and perform analysis.
4. You can run step2_kafka_consumer_py_script.py in another seperate terminal to debug the data coming from kafka

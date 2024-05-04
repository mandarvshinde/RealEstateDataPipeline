import json
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

# Initialize Kafka Consumer
consumer_conf = {
    'bootstrap.servers': ':9092',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['real-estate-banglore'])

# MongoDB settings
mongo_client = MongoClient('localhost', 27017)
mongo_db = mongo_client['DP_Kafka']  # Replace 'your_database_name' with your MongoDB database name
mongo_collection = mongo_db['mongorebang']  # Replace 'your_collection_name' with your MongoDB collection name

# Function to insert data into MongoDB
def insert_into_mongodb(data):
    try:
        mongo_collection.insert_one(data)
        print("Data inserted into MongoDB successfully")
    except Exception as e:
        print(f"Error inserting data into MongoDB: {e}")

# Consume messages from Kafka and insert into MongoDB
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                continue
            else:
                print(f"Kafka error: {msg.error()}")
                break
        else:
            # Process the message
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Received message from Kafka: {data}")
            insert_into_mongodb(data)
finally:
    # Close Kafka consumer and MongoDB connection
    consumer.close()
    mongo_client.close()

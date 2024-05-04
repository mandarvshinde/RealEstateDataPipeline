import json
import pandas as pd
from time import sleep
from confluent_kafka import Producer

# Initialize the Kafka producer object
kafka_producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(kafka_producer_config)

# Read the data from CSV file and convert it to a list of dictionaries
city_data = pd.read_csv('D:\\Data Pipeline\\real-estate-data-from-7-indian-cities\\REData.csv')
cities = city_data.to_json(orient='records')  # Convert DataFrame to JSON
city_records = json.loads(cities)  # Convert JSON to Python list of dictionaries

# Print CSV data
print("CSV Data:")
print(city_data)

# Callback function to handle delivery reports
def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to topic {msg.topic()} partition {msg.partition()}')

# Iterate over the list of dictionaries and send each record to the Kafka Cluster
for city in city_records:
    record_value = json.dumps(city)  # Convert dictionary to JSON string
    producer.produce('real-estate-banglore', value=record_value, callback=delivery_callback)
    producer.poll(0)  # Trigger delivery report callbacks
    sleep(1)  # Introduce a delay to control message sending rate

# Wait for all messages to be delivered before exiting
producer.flush()

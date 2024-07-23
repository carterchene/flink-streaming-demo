from kafka import KafkaProducer
import json
import time
from datagen.fake_data_generator import generate_telemetry

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'], # kafka running in docker container
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # convert data to json, send to kafka
)

def send_to_kafka():
    while True:
        well_data = generate_telemetry()
        producer.send('oil-gas-telemetry', value=well_data)
        time.sleep(3)

send_to_kafka()
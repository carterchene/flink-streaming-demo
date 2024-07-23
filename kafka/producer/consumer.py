from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'oil-gas-telemetry',
    bootstrap_servers=['localhost:29092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    print(f"Received: {message.value}")
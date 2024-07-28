from kafka import KafkaConsumer
import json

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic = 'listen_events'
group_id = 'test-consumer-group'

# Create Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=group_id,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Connected to Kafka broker at {bootstrap_servers}")
print(f"Listening for messages on topic: {topic}")
print("Waiting for messages... (Press Ctrl+C to stop)")

# Consume messages
try:
    for message in consumer:
        print(f"Received message: {message.value}")
except KeyboardInterrupt:
    print("Consumer stopped.")
finally:
    consumer.close()
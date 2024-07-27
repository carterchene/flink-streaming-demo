from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

# Kafka broker address - replace with your EC2 instance's public DNS or IP
bootstrap_servers = ['localhost:9092']

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    request_timeout_ms = 30000,
    security_protocol = 'PLAINTEXT'
    
)

# List of sample user activities
activities = ['login', 'logout', 'purchase', 'page_view', 'add_to_cart']

def generate_data():
    return {
        'user_id': random.randint(1, 1000),
        'timestamp': datetime.now().isoformat(),
        'activity': random.choice(activities),
        'details': {
            'page': f'/page_{random.randint(1, 100)}',
            'duration': random.randint(1, 600)
        }
    }

# Kafka topic name
topic = 'test-topic'

# Generate and send data
try:
    while True:
        # Generate a message
        message = generate_data()
        
        # Send the message
        future = producer.send(topic, value=message)
        
        # Block for 'synchronous' sends
        record_metadata = future.get(timeout=30)
        
        print(f"Sent message: {message}")
        print(f"Topic: {record_metadata.topic}")
        print(f"Partition: {record_metadata.partition}")
        print(f"Offset: {record_metadata.offset}")
        print("--------------------")
        
        # Wait for a short time before sending the next message
        time.sleep(1)

except KeyboardInterrupt:
    print("Stopping the producer...")
    producer.close()
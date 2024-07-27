import os
from streaming_functions import create_or_get_flink_env, create_kafka_read_stream, process_stream, create_file_write_stream

# Kafka Topics
LISTEN_EVENTS_TOPIC = "listen_events"
PAGE_VIEW_EVENTS_TOPIC = "page_view_events"
AUTH_EVENTS_TOPIC = "auth_events"

KAFKA_PORT = "9092"
KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", 'localhost')
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", 'streaming-demo')
S3_STORAGE_PATH = f's3://{AWS_S3_BUCKET}'

# Initialize a Flink Stream Table Environment
t_env = create_or_get_flink_env('Eventsim Stream')

# Listen events stream
listen_events = create_kafka_read_stream(t_env, KAFKA_ADDRESS, KAFKA_PORT, LISTEN_EVENTS_TOPIC)
listen_events = process_stream(listen_events, schemas[LISTEN_EVENTS_TOPIC], LISTEN_EVENTS_TOPIC)

# Page view stream
page_view_events = create_kafka_read_stream(t_env, KAFKA_ADDRESS, KAFKA_PORT, PAGE_VIEW_EVENTS_TOPIC)
page_view_events = process_stream(page_view_events, schemas[PAGE_VIEW_EVENTS_TOPIC], PAGE_VIEW_EVENTS_TOPIC)

# Auth stream
auth_events = create_kafka_read_stream(t_env, KAFKA_ADDRESS, KAFKA_PORT, AUTH_EVENTS_TOPIC)
auth_events = process_stream(auth_events, schemas[AUTH_EVENTS_TOPIC], AUTH_EVENTS_TOPIC)

# Write a file to storage every 2 minutes in Parquet format
create_file_write_stream(listen_events, f"{S3_STORAGE_PATH}/{LISTEN_EVENTS_TOPIC}", f"{S3_STORAGE_PATH}/checkpoint/{LISTEN_EVENTS_TOPIC}")
create_file_write_stream(page_view_events, f"{S3_STORAGE_PATH}/{PAGE_VIEW_EVENTS_TOPIC}", f"{S3_STORAGE_PATH}/checkpoint/{PAGE_VIEW_EVENTS_TOPIC}")
create_file_write_stream(auth_events, f"{S3_STORAGE_PATH}/{AUTH_EVENTS_TOPIC}", f"{S3_STORAGE_PATH}/checkpoint/{AUTH_EVENTS_TOPIC}")

# Execute the Flink pipeline
t_env.execute("Stream Processing")

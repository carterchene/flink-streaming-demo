from dataclasses import asdict, dataclass, field
from typing import List, Dict
import os
from jinja2 import Environment, FileSystemLoader
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# Environment variables
KAFKA_BROKER_IP = os.getenv('KAFKA_BROKER_IP')
S3_BUCKET = os.getenv('S3_BUCKET')

@dataclass(frozen=True)
class StreamJobConfig:
    job_name: str = 'stream-all-events-job'
    jars: List[str] = field(default_factory=lambda: [
        "file:///lib/flink-sql-connector-kafka-3.2.0-1.18.jar", # lib is the shared directory between emr nodes
        "file:///lib/flink-sql-parquet-1.18.1.jar",
        "file:///lib/flink-parquet-1.18.1.jar"
    ])
    checkpoint_interval: int = 300  # 5 minutes
    checkpoint_pause: int = 150  # 2.5 minutes
    checkpoint_timeout: int = 900  # 15 minutes

@dataclass(frozen=True)
class KafkaConfig:
    connector: str = 'kafka'
    bootstrap_servers: str = f'{KAFKA_BROKER_IP}:9092'
    scan_startup_mode: str = 'latest-offset'
    consumer_group_id: str = 'flink-consumer-group-1'
    format: str = 'json'

@dataclass(frozen=True)
class S3SinkConfig:
    connector: str = 'filesystem'
    path: str = f's3://{S3_BUCKET}/outputs/{{table_name}}'
    format: str = 'parquet'
    file_size: str = '128MB'
    rollover_interval: str = '5 min'
    check_interval: str = '5 min'
    partition_pattern: str = '$year-$month-$day 00:00:00'
    commit_delay: str = '10 min'
    commit_policy: str = 'success-file'
    commit_trigger: str = 'process-time'
    time_zone: str = 'UTC'

def get_execution_environment(config: StreamJobConfig) -> StreamTableEnvironment:
    s_env = StreamExecutionEnvironment.get_execution_environment()
    for jar in config.jars:
        s_env.add_jars(jar)
    s_env.enable_checkpointing(config.checkpoint_interval * 1000)
    s_env.get_checkpoint_config().set_min_pause_between_checkpoints(config.checkpoint_pause * 1000)
    s_env.get_checkpoint_config().set_checkpoint_timeout(config.checkpoint_timeout * 1000)
    
    t_env = StreamTableEnvironment.create(s_env)
    job_config = t_env.get_config().get_configuration()
    job_config.set_string("pipeline.name", config.job_name)
    return t_env

def get_sql_query(entity: str, type: str, config: Dict, template_env: Environment) -> str:
    return template_env.get_template(f"{type}/{entity}.sql").render(config)

def stream_events(t_env: StreamTableEnvironment, event_types: List[str], template_env: Environment):
    kafka_config = asdict(KafkaConfig())
    s3_sink_config = asdict(S3SinkConfig())

    for event_type in event_types:
        print(f'Streaming {event_type}...')
        
        # Create source table
        source_config = {**kafka_config, 'topic': event_type}
        t_env.execute_sql(get_sql_query(event_type, 'source', source_config, template_env))
        
        # Create sink table
        sink_config = {**s3_sink_config, 'table_name': event_type}
        t_env.execute_sql(get_sql_query(event_type, 'sink', sink_config, template_env))
        
        # Create insert statement
        stmt_set = t_env.create_statement_set()
        stmt_set.add_insert_sql(get_sql_query(event_type, 'insert', {'source': f'kafka_{event_type}', 'sink': event_type}, template_env))
        
        job = stmt_set.execute()
        print(f"Job status for {event_type}: {job.get_job_client().get_job_status()}")

if __name__ == '__main__':
    config = StreamJobConfig()
    t_env = get_execution_environment(config)
    
    template_env = Environment(loader=FileSystemLoader("sql_templates/"))
    
    event_types = ['listen_events', 'auth_events', 'page_view_events', 'status_change_events']
    
    stream_events(t_env, event_types, template_env)
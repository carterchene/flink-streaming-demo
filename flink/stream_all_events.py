from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col, lit
import os

KAFKA_BROKER_IP = os.getenv('KAFKA_BROKER_IP')
S3_BUCKET = os.getenv('S3_BUCKET')

def construct_insert_statement(source, sink) -> str:
    return f"""
            INSERT INTO {sink} 
                SELECT 
                *, 
                DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`ts`/1000)), 'yyyy') as `year`,
                DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`ts`/1000)), 'MM') as `month`,
                DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`ts`/1000)), 'dd') as `day`,
                DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`ts`/1000)), 'HH') as `hour`
            FROM {source}
            """

def create_sink_table(t_env, table_name, schema):
    t_env.execute_sql(f"""
        CREATE TABLE {table_name} (
            {schema},
            `year` STRING,
            `month` STRING,
            `day` STRING,
            `hour` STRING
        ) PARTITIONED BY (`year`, `month`, `day`, `hour`) WITH (
            'connector' = 'filesystem',
            'path' = 's3://{S3_BUCKET}/outputs/{table_name}',
            'format' = 'parquet',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '5 min',
            'sink.rolling-policy.check-interval' = '5 min',
            'partition.time-extractor.timestamp-pattern'='$year-$month-$day 00:00:00',
            'sink.partition-commit.delay'='10 min',
            'sink.partition-commit.policy.kind'='success-file',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.watermark-time-zone' = 'UTC'
        )
    """)

def stream_events(t_env, source_table, sink_table, schema):
    t_env.execute_sql(f"""
        CREATE TABLE {source_table} (
            {schema}
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{source_table.replace("kafka_", "")}',
            'properties.bootstrap.servers' = '{KAFKA_BROKER_IP}:9092',
            'properties.group.id' = 'flink-consumer-group-1',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    create_sink_table(t_env, sink_table, schema)

    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(construct_insert_statement(source_table, sink_table))
    
    job = stmt_set.execute()
    print(f"Job status for {sink_table}: {job.get_job_client().get_job_status()}")

if __name__ == '__main__':
    s_env = StreamExecutionEnvironment.get_execution_environment()
    
    s_env.enable_checkpointing(300000)  # 5 minutes
    s_env.get_checkpoint_config().set_min_pause_between_checkpoints(150000)  # 2.5 minutes
    s_env.get_checkpoint_config().set_checkpoint_timeout(900000)  # 15 minutes
    
    s_env.add_jars("file:///lib/flink-sql-connector-kafka-3.2.0-1.18.jar")
    s_env.add_jars("file:///lib/flink-sql-parquet-1.18.1.jar")
    s_env.add_jars("file:///lib/flink-parquet-1.18.1.jar")
    
    t_env = StreamTableEnvironment.create(s_env)
    
    schemas = {
        'listen_events': """
            `song` STRING, `artist` STRING, `duration` DOUBLE, `ts` BIGINT,
            `sessionId` INTEGER, `auth` STRING, `level` STRING, `itemInSession` INTEGER,
            `city` STRING, `zip` STRING, `state` STRING, `userAgent` STRING,
            `lon` DOUBLE, `lat` DOUBLE, `userId` INTEGER, `lastName` STRING,
            `firstName` STRING, `gender` STRING, `registration` BIGINT
        """,
        'auth_events': """
            `ts` BIGINT, `sessionId` STRING, `level` STRING, `itemInSession` INT,
            `city` STRING, `zip` STRING, `state` STRING, `userAgent` STRING,
            `lon` DOUBLE, `lat` DOUBLE, `userId` STRING, `lastName` STRING,
            `firstName` STRING, `gender` STRING, `registration` BIGINT, `success` BOOLEAN
        """,
        'page_view_events': """
            `ts` BIGINT, `sessionId` STRING, `page` STRING, `auth` STRING,
            `method` STRING, `status` INT, `level` STRING, `itemInSession` INT,
            `city` STRING, `zip` STRING, `state` STRING, `userAgent` STRING,
            `lon` DOUBLE, `lat` DOUBLE, `userId` STRING, `lastName` STRING,
            `firstName` STRING, `gender` STRING, `registration` BIGINT,
            `artist` STRING, `song` STRING, `duration` DOUBLE
        """,
        'status_change_events': """
            `ts` BIGINT, `sessionId` STRING, `auth` STRING, `level` STRING,
            `itemInSession` INT, `city` STRING, `zip` STRING, `state` STRING,
            `userAgent` STRING, `lon` DOUBLE, `lat` DOUBLE, `userId` STRING,
            `lastName` STRING, `firstName` STRING, `gender` STRING, `registration` BIGINT
        """
    }

    for event_type, schema in schemas.items():
        print(f'Streaming {event_type}...')
        stream_events(t_env, f'kafka_{event_type}', event_type, schema)
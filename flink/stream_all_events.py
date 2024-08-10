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
                DATE_FORMAT(TO_TIMESTAMP(FROM_UNIXTIME(`ts`/1000)), 'dd') as `day`
            FROM {source}
            """


def stream_listen_events(t_env):
    
    sink_table = 'listen_events'
    source_table = 'kafka_listen_events'

    t_env.execute_sql(f"""
        CREATE TABLE {source_table} (
            `song` string,
            `artist` string,
            `duration` double,
            `ts` bigint,
            `sessionId` INTEGER,
            `auth` string,
            `level` string,
            `itemInSession` INTEGER,
            `city` string,
            `zip` string,
            `state` string,
            `userAgent` string,
            `lon` double,
            `lat` double,
            `userId` integer,
            `lastName` string,
            `firstName` string,
            `gender` string,
            `registration` BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'listen_events',
            'properties.bootstrap.servers' = '{KAFKA_BROKER_IP}:9092',
            'properties.group.id' = 'flink-consumer-group-1',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE {sink_table} (
            `song` string,
            `artist` string,
            `duration` double,
            `ts` bigint,
            `sessionId` INTEGER,
            `auth` string,
            `level` string,
            `itemInSession` INTEGER,
            `city` string,
            `zip` string,
            `state` string,
            `userAgent` string,
            `lon` double,
            `lat` double,
            `userId` integer,
            `lastName` string,
            `firstName` string,
            `gender` string,
            `registration` BIGINT,
            `year` STRING,
            `month` STRING,
            `day` STRING
        ) PARTITIONED BY (`year`, `month`, `day`) WITH (
            'connector' = 'filesystem',
            'path' = 's3://streaming-demo-project/outputs/{sink_table}',
            'format' = 'parquet',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '5 min',
            'sink.rolling-policy.check-interval' = '1 min',
            'partition.time-extractor.timestamp-pattern'='$year-$month-$day 00:00:00',
            'sink.partition-commit.delay'='5 min',
            'sink.partition-commit.policy.kind'='success-file',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.watermark-time-zone' = 'UTC'
        )
    """)

    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(construct_insert_statement(source_table, sink_table))
    
    job = stmt_set.execute()
    print(f"Job status: {job.get_job_client().get_job_status()}")


def stream_auth_events(t_env):

    sink_table = 'auth_events'
    source_table = 'kafka_auth_events'

    t_env.execute_sql(f"""
        CREATE TABLE {source_table} (
            `ts` BIGINT,
            `sessionId` STRING,
            `level` STRING,
            `itemInSession` INT,
            `city` STRING,
            `zip` STRING,
            `state` STRING,
            `userAgent` STRING,
            `lon` DOUBLE,
            `lat` DOUBLE,
            `userId` STRING,
            `lastName` STRING,
            `firstName` STRING,
            `gender` STRING,
            `registration` BIGINT,
            `success` BOOLEAN 
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'auth_events',
            'properties.bootstrap.servers' = '{KAFKA_BROKER_IP}:9092',
            'properties.group.id' = 'flink-consumer-group-1',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE {sink_table} (
            `ts` BIGINT,
            `sessionId` STRING,
            `level` STRING,
            `itemInSession` INT,
            `city` STRING,
            `zip` STRING,
            `state` STRING,
            `userAgent` STRING,
            `lon` DOUBLE,
            `lat` DOUBLE,
            `userId` STRING,
            `lastName` STRING,
            `firstName` STRING,
            `gender` STRING,
            `registration` BIGINT,
            `success` BOOLEAN, 
            `year` STRING,
            `month` string, 
            `day` string
        ) PARTITIONED BY (`year`, `month`, `day`) WITH (
            'connector' = 'filesystem',
            'path' = 's3://streaming-demo-project/outputs/{sink_table}',
            'format' = 'parquet',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '5 min',
            'sink.rolling-policy.check-interval' = '1 min',
            'partition.time-extractor.timestamp-pattern'='$year-$month-$day 00:00:00',
            'sink.partition-commit.delay'='5 min',
            'sink.partition-commit.policy.kind'='success-file',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.watermark-time-zone' = 'UTC'
        )
    """)

    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(construct_insert_statement(source_table, sink_table))
    
    job = stmt_set.execute()
    print(f"Job status: {job.get_job_client().get_job_status()}")

def stream_page_view_events(t_env):

    sink_table = 'page_view_events'
    source_table = f'kafka_{sink_table}'

    t_env.execute_sql(f"""
        CREATE TABLE {source_table} (
            `ts` BIGINT,
            `sessionId` STRING,
            `page` STRING,
            `auth` STRING,
            `method` STRING,
            `status` INT,
            `level` STRING,
            `itemInSession` INT,
            `city` STRING,
            `zip` STRING,
            `state` STRING,
            `userAgent` STRING,
            `lon` DOUBLE,
            `lat` DOUBLE,
            `userId` STRING,
            `lastName` STRING,
            `firstName` STRING,
            `gender` STRING,
            `registration` BIGINT,
            `artist` STRING,
            `song` STRING,
            `duration` DOUBLE 
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'page_view_events',
            'properties.bootstrap.servers' = '{KAFKA_BROKER_IP}:9092',
            'properties.group.id' = 'flink-consumer-group-1',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE {sink_table} (
            `ts` BIGINT,
            `sessionId` STRING,
            `page` STRING,
            `auth` STRING,
            `method` STRING,
            `status` INT,
            `level` STRING,
            `itemInSession` INT,
            `city` STRING,
            `zip` STRING,
            `state` STRING,
            `userAgent` STRING,
            `lon` DOUBLE,
            `lat` DOUBLE,
            `userId` STRING,
            `lastName` STRING,
            `firstName` STRING,
            `gender` STRING,
            `registration` BIGINT,
            `artist` STRING,
            `song` STRING,
            `duration` DOUBLE 
            `year` STRING,
            `month` string, 
            `day` string
        ) PARTITIONED BY (`year`, `month`, `day`) WITH (
            'connector' = 'filesystem',
            'path' = 's3://streaming-demo-project/outputs/{sink_table}',
            'format' = 'parquet',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '5 min',
            'sink.rolling-policy.check-interval' = '1 min',
            'partition.time-extractor.timestamp-pattern'='$year-$month-$day 00:00:00',
            'sink.partition-commit.delay'='5 min',
            'sink.partition-commit.policy.kind'='success-file',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.watermark-time-zone' = 'UTC'
        )
    """)

    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(construct_insert_statement(source_table, sink_table))
    
    job = stmt_set.execute()
    print(f"Job status: {job.get_job_client().get_job_status()}")

def stream_status_change_events(t_env):

    sink_table = 'status_change_events'
    source_table = f'kafka_{sink_table}'

    t_env.execute_sql(f"""
        CREATE TABLE {source_table} (
            `ts` BIGINT,
            `sessionId` STRING,
            `auth` STRING,
            `level` STRING,
            `itemInSession` INT,
            `city` STRING,
            `zip` STRING,
            `state` STRING,
            `userAgent` STRING,
            `lon` DOUBLE,
            `lat` DOUBLE,
            `userId` STRING,
            `lastName` STRING,
            `firstName` STRING,
            `gender` STRING,
            `registration` BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'status_change_events',
            'properties.bootstrap.servers' = '{KAFKA_BROKER_IP}:9092',
            'properties.group.id' = 'flink-consumer-group-1',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE {sink_table} (
            `ts` BIGINT,
            `sessionId` STRING,
            `auth` STRING,
            `level` STRING,
            `itemInSession` INT,
            `city` STRING,
            `zip` STRING,
            `state` STRING,
            `userAgent` STRING,
            `lon` DOUBLE,
            `lat` DOUBLE,
            `userId` STRING,
            `lastName` STRING,
            `firstName` STRING,
            `gender` STRING,
            `registration` BIGINT
            `year` STRING,
            `month` string, 
            `day` string
        ) PARTITIONED BY (`year`, `month`, `day`) WITH (
            'connector' = 'filesystem',
            'path' = 's3://streaming-demo-project/outputs/{sink_table}',
            'format' = 'parquet',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '5 min',
            'sink.rolling-policy.check-interval' = '1 min',
            'partition.time-extractor.timestamp-pattern'='$year-$month-$day 00:00:00',
            'sink.partition-commit.delay'='5 min',
            'sink.partition-commit.policy.kind'='success-file',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.watermark-time-zone' = 'UTC'
        )
    """)

    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql(construct_insert_statement(source_table, sink_table))
    
    job = stmt_set.execute()
    print(f"Job status: {job.get_job_client().get_job_status()}")

if __name__ == '__main__':
    # Set up the streaming environment
    s_env = StreamExecutionEnvironment.get_execution_environment()
    
    # Enable checkpointing
    s_env.enable_checkpointing(60000)  # 60 seconds
    s_env.get_checkpoint_config().set_min_pause_between_checkpoints(30000)
    s_env.get_checkpoint_config().set_checkpoint_timeout(900000)
    
    # Add the required JAR files
    s_env.add_jars("file:///lib/flink-sql-connector-kafka-3.2.0-1.18.jar")
    s_env.add_jars("file:///lib/flink-sql-parquet-1.18.1.jar")
    s_env.add_jars("file:///lib/flink-parquet-1.18.1.jar")
    
    # Create the table environment
    t_env = StreamTableEnvironment.create(s_env)
    
    print('Streaming listen events...')
    stream_listen_events(t_env)
    
    print('Streaming auth events...')
    stream_auth_events(t_env)

    print('Streaming page view events...')
    stream_page_view_events(t_env)
    
    print('Streaming status change events...')
    stream_status_change_events(t_env)

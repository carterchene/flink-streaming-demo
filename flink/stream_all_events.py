from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.table.expressions import col, lit

def write_to_s3(t_env):
    
    t_env.execute_sql("""
        CREATE TABLE kafka_source (
            `song` string ,
            `artist` string ,
            `duration` double ,
            `ts` bigint ,
            `sessionId` INTEGER ,
            `auth` string ,
            `level` string ,
            `itemInSession` INTEGER ,
            `city` string ,
            `zip` string ,
            `state` string ,
            `userAgent` string ,
            `lon` double ,
            `lat` double ,
            `userId` integer ,
            `lastName` string ,
            `firstName` string ,
            `gender` string ,
            `registration` BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'listen_events',
            'properties.bootstrap.servers' = '35.183.28.240:9092',
            'properties.group.id' = 'flink-consumer-group-1', 
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE listen_events_s3 (
            `song` string ,
            `artist` string ,
            `duration` double ,
            `ts` bigint ,
            `sessionId` INTEGER ,
            `auth` string ,
            `level` string ,
            `itemInSession` INTEGER ,
            `city` string ,
            `zip` string ,
            `state` string ,
            `userAgent` string ,
            `lon` double ,
            `lat` double ,
            `userId` integer ,
            `lastName` string ,
            `firstName` string ,
            `gender` string ,
            `registration` BIGINT
            ) WITH (
                   'connector' = 'filesystem',
                'path' = 's3://streaming-demo-project/outputs',
                'format' = 'parquet',
                'sink.rolling-policy.file-size' = '128MB',
                'sink.rolling-policy.rollover-interval' = '15 min',
                'sink.rolling-policy.check-interval' = '1 min',
                'partition.time-extractor.timestamp-pattern'='$year-$month-$day 00:00:00',
                'sink.partition-commit.delay'='1 h',
                'sink.partition-commit.policy.kind'='success-file'
            )
    """)
    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql("""INSERT INTO listen_events_s3 SELECT * FROM kafka_source;""")
    
    job = stmt_set.execute()

    print(
          f"""Job status: {job.get_job_client().get_job_status()}"""
    )

    # t_env.execute('stream to s3')



if __name__ == '__main__':

    # set the streaming environment
    s_env = StreamExecutionEnvironment.get_execution_environment()
    # add the kafka connector 
    # s_env.add_jars("file:///C:/Users/Carter%20Dakota/portfolio/downloads/flink-connector-kafka-3.2.0-1.18.jar")
    # s_env.add_jars("file:///C:/Users/Carter%20Dakota/portfolio/downloads/flink-s3-fs-presto-1.19.1.jar")
    # s_env.add_jars("file:///C:/Users/Carter%20Dakota/portfolio/downloads/flink-sql-parquet-1.18.1.jar")
    s_env.add_jars("file:///lib/flink-sql-connector-kafka-3.2.0-1.18.jar")
    s_env.add_jars("file:///lib/flink-sql-parquet-1.18.1.jar")
    s_env.add_jars("file:///lib/flink-parquet-1.18.1.jar")

    t_env = StreamTableEnvironment.create(s_env)

    print('writing to s3...')
    write_to_s3(t_env)


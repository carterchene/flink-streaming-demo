from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.table import StreamTableEnvironment

s_env = StreamExecutionEnvironment.get_execution_environment()
s_env.add_jars("file:///C:/Users/Carter%20Dakota/portfolio/downloads/flink-sql-connector-kafka-3.2.0-1.19.jar")
# start a checkpoint every 10,000 ms (10 s)
s_env.enable_checkpointing(10 * 1000)
# make sure 5000 ms (5 s) of progress happen between checkpoints
s_env.get_checkpoint_config().set_min_pause_between_checkpoints(
    5 * 1000
)
# checkpoints have to complete within 5 minute, or are discarded
s_env.get_checkpoint_config().set_checkpoint_timeout(
    5 * 1000
)
execution_config = s_env.get_config()
# execution_config.set_parallelism('1')

t_env = StreamTableEnvironment.create(s_env)

# deserialization_schema = JsonRowDeserializationSchema.builder() \
#     .type_info(type_info=Types.ROW([Types.INT(), Types.STRING()])).build()

# can test the ec2 instance kafka server from here using the external ip it
# kafka_consumer = FlinkKafkaConsumer(
#     topics='listen_events',
#     deserialization_schema=deserialization_schema,
#     properties={'bootstrap.servers': ':9092'})
# 
# ds = env.add_source(kafka_consumer)

t_env.execute_sql("""
    CREATE TABLE kafka_source (
        event_data STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'listen_events',
        'properties.bootstrap.servers' = 'EXTERNAL_ADDRESS_HERE:9092',
        'properties.group.id' = 'flink-consumer-group-1', 
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'raw'
    )
""")

t_env.execute_sql("""
    CREATE TABLE print_sink (
        event_data STRING
    ) WITH (
        'connector' = 'print'
    )
""")
print('straeting flink job to transfer from kafka topic to print sink')
t_env.execute_sql("INSERT INTO print_sink SELECT * FROM kafka_source").wait()

# t_env.execute_sql("""
#     CREATE TABLE csv_sink (
#         event_data STRING
#     ) WITH (
#         'connector' = 'filesystem',
#         'path' = 'file:///C:/Users/Carter%20Dakota/portfolio/downloads/output.csv',
#         'format' = 'csv'
#     )
# """)

# t_env.execute_sql("INSERT INTO csv_sink SELECT * FROM kafka_source").wait()


#with ds.execute_and_collect() as results:
#    for result in results:
#        print(result)
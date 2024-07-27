from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.udf import udf
from pyflink.table.expressions import col, lit, year, month, dayofmonth, hour
from pyflink.table import TableDescriptor, Schema
from pyflink.table.filesystem import FileSystem

@udf(result_type=DataTypes.STRING())
def string_decode(s, encoding='utf-8'):
    if s:
        return (s.encode('latin1')
                .decode('unicode-escape')
                .encode('latin1')
                .decode(encoding)
                .strip('"'))
    else:
        return s

def create_or_get_flink_env(app_name):
    """
    Creates or gets a Flink Stream Table Environment

    Parameters:
        app_name : str
            Pass the name of your app
    Returns:
        t_env: StreamTableEnvironment
    """
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=settings)
    t_env.get_config().get_configuration().set_string("pipeline.name", app_name)
    return t_env

def create_kafka_read_stream(t_env, kafka_address, kafka_port, topic):
    """
    Creates a Kafka read stream

    Parameters:
        t_env : StreamTableEnvironment
            A StreamTableEnvironment object
        kafka_address: str
            Host address of the Kafka bootstrap server
        topic : str
            Name of the Kafka topic
    Returns:
        read_stream: Table
    """
    kafka_source = TableDescriptor.for_connector("kafka")
    kafka_source = kafka_source.option("properties.bootstrap.servers", f"{kafka_address}:{kafka_port}")
    kafka_source = kafka_source.option("scan.startup.mode", "earliest-offset")
    kafka_source = kafka_source.format("json")
    kafka_source = kafka_source.schema(schemas[topic])
    kafka_source = kafka_source.option("topic", topic)
    
    t_env.create_temporary_table(topic, kafka_source.build())
    return t_env.from_path(topic)

def process_stream(stream, stream_schema, topic):
    """
    Process stream to fetch the value from the Kafka message.
    Convert ts to timestamp format and produce year, month, day, hour columns.

    Parameters:
        stream : Table
            The data stream table for your stream
    Returns:
        stream: Table
    """
    stream = (stream
              .add_columns((col("ts") / 1000).cast(DataTypes.TIMESTAMP(3)).alias("timestamp"))
              .add_columns(year(col("timestamp")).alias("year"))
              .add_columns(month(col("timestamp")).alias("month"))
              .add_columns(dayofmonth(col("timestamp")).alias("day"))
              .add_columns(hour(col("timestamp")).alias("hour")))

    if topic in ["listen_events", "page_view_events"]:
        stream = (stream
                  .add_columns(string_decode(col("song")).alias("song"))
                  .add_columns(string_decode(col("artist")).alias("artist")))

    return stream

def create_file_write_stream(stream, storage_path, checkpoint_path, trigger="2 minutes"):
    """
    Write the stream back to a file store

    Parameters:
        stream : Table
            The data stream table for your stream
        storage_path : str
            The file output path
        checkpoint_path : str
            The checkpoint location for Flink
        trigger : str
            The trigger interval
    """
    file_sink = TableDescriptor.for_connector("filesystem")
    file_sink = file_sink.option("path", storage_path)
    file_sink = file_sink.option("sink.partition-commit.delay", "0s")
    file_sink = file_sink.option("sink.partition-commit.policy.kind", "success-file")
    file_sink = file_sink.format("parquet")
    file_sink = file_sink.schema(stream.get_schema())
    
    stream.execute_insert(file_sink.build())

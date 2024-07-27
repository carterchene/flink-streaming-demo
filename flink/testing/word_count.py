from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import Schema, OldCsv
from pyflink.table.descriptors import FileSystem as FileSystemDescriptor

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)

    # Define the source schema
    t_env.connect(FileSystemDescriptor().path('s3://streaming-demo-project/input.txt')) \
        .with_format(OldCsv()
                     .field('word', DataTypes.STRING())) \
        .with_schema(Schema()
                     .field('word', DataTypes.STRING())) \
        .create_temporary_table('source')

    # Define the sink schema
    t_env.connect(FileSystemDescriptor().path('s3://streaming-demo-project/output')) \
        .with_format(OldCsv()
                     .field_delimiter('\t')
                     .field('word', DataTypes.STRING())
                     .field('count', DataTypes.BIGINT())) \
        .with_schema(Schema()
                     .field('word', DataTypes.STRING())
                     .field('count', DataTypes.BIGINT())) \
        .create_temporary_table('sink')

    # Run the word count
    t_env.from_path('source') \
        .group_by('word') \
        .select('word, COUNT(1) as count') \
        .insert_into('sink')

    t_env.execute('word count')

if __name__ == '__main__':
    main()
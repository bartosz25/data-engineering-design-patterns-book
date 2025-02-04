from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import get_base_dir, get_delta_users_table_dir

if __name__ == '__main__':
    spark_session_builder = (SparkSession.builder.master('local[*]')
                             .config('spark.sql.extensions',
                                     'io.delta.sql.DeltaSparkSessionExtension')
                             .config('spark.sql.catalog.spark_catalog',
                                     'org.apache.spark.sql.delta.catalog.DeltaCatalog'))
    spark_session = (configure_spark_with_delta_pip(spark_session_builder=spark_session_builder,
                                                    extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0']
                                                    ).getOrCreate())

    input_data_stream = spark_session.readStream \
        .option('kafka.bootstrap.servers', 'localhost:9094') \
        .option('subscribe', 'users_context') \
        .option('includeHeaders', 'true') \
        .option('startingOffsets', 'EARLIEST') \
        .format('kafka').load()

    visit_schema = '''
        user_id STRING, ip STRING, login STRING, connected_since TIMESTAMP
    '''
    context_from_kafka = (
        input_data_stream.select(F.from_json(F.col('value').cast('string'), visit_schema).alias('visit'))
        .selectExpr('visit.*'))


    def save_most_recent_user_context(context_to_save: DataFrame, batch_number: int):
        deduplicated_context = context_to_save.dropDuplicates(['user_id']).alias('new')

        current_table = DeltaTable.forPath(spark_session, get_delta_users_table_dir())

        (current_table.alias('current')
         .merge(deduplicated_context, 'current.user_id = new.user_id')
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute()
         )


    delta_table_writer = (context_from_kafka.writeStream
                          .option('checkpointLocation', f'{get_base_dir()}/checkpoint-delta')
                          .foreachBatch(save_most_recent_user_context))

    delta_table_writer.start().awaitTermination()

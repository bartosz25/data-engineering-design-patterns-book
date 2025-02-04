from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, TimestampType, StringType

from config import get_base_dir

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = spark.readStream \
        .option('kafka.bootstrap.servers', 'localhost:9094') \
        .option('subscribe', 'visits') \
        .option('startingOffsets', 'EARLIEST') \
        .format('kafka').load()

    visit_schema = '''
        visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
        context STRUCT<
            referral STRING, ad_id STRING, 
            user STRUCT<
                ip STRING, login STRING, connected_since TIMESTAMP
            >,
            technical STRUCT<
                browser STRING, browser_version STRING, network_type STRING, device_type STRING, device_version STRING
            >
        >
    '''
    visits_from_kafka = (input_data_stream
                         .select(F.from_json(F.col('value').cast('string'), visit_schema).alias('visit'))
                         .selectExpr('visit.*')
                         )


    def split_visit_attributes(visits_to_save: DataFrame, batch_number: int):
        visits_to_save.cache()

        visits_without_user_context = (
            visits_to_save
            .filter('user_id IS NOT NULL AND context.user.login IS NOT NULL')
            .withColumn('context', F.col('context').dropFields('user'))
            .select(F.col('visit_id').alias('key'),
                    F.to_json(F.struct('*')).alias('value'))
        )

        (visits_without_user_context.write.format('kafka')
         .option('kafka.bootstrap.servers', 'localhost:9094')
         .option('topic', 'visits_raw')
         .save())

        user_context_to_save = (visits_to_save.selectExpr('context.user.*', 'user_id')
        .select(
            F.col('user_id').alias('key'),
            F.to_json(F.struct('*')).alias('value')
        ))

        (user_context_to_save.write.format('kafka')
         .option('kafka.bootstrap.servers', 'localhost:9094')
         .option('topic', 'users_context')
         .save())

        visits_to_save.unpersist()


    write_data_stream = (visits_from_kafka.writeStream
                         .option('checkpointLocation', f'{get_base_dir()}/checkpoint')
                         .foreachBatch(split_visit_attributes))

    write_data_stream.start().awaitTermination()

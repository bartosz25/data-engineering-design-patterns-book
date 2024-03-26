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
        .option('includeHeaders', 'true') \
        .option('startingOffsets', 'EARLIEST') \
        .format('kafka').load()

    event_schema = StructType([
        StructField('visit_id', StringType()),
        StructField('event_time', TimestampType())
    ])

    deduplicated_visits = (input_data_stream
                           .select(F.col('value').cast('string'))
                           .select(F.from_json('value', event_schema).alias('value_struct'), 'value')
                           .select('value_struct.event_time', 'value_struct.visit_id', 'value')
                           .withWatermark('event_time', '10 minutes')
                           .dropDuplicates(['visit_id', 'event_time'])
                           # keep only the value which is required for the sink!)
                           .drop('event_time', 'visit_id'))

    def generate_files(deduplicated_visits_to_save: DataFrame, batch_number: int):
        (deduplicated_visits_to_save.write.format('text').mode('overwrite')
         .save(f'{get_base_dir()}/output/{batch_number}'))

    write_data_stream = (deduplicated_visits.writeStream
                         .option('checkpointLocation', f'{get_base_dir()}/checkpoint')
                         .foreachBatch(generate_files))

    write_data_stream.start().awaitTermination()

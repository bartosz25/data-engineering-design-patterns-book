from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, TimestampType, StringType

from config import get_base_dir

if __name__ == '__main__':
    spark = (SparkSession.builder.master('local[*]')
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
        .getOrCreate())

    input_data_stream = (spark.readStream
        .option('kafka.bootstrap.servers', 'localhost:9094')
        .option('subscribe', 'visits-decorated')
        .option('includeHeaders', 'true')
        .option('startingOffsets', 'EARLIEST')
        .format('kafka').load())

    job_version = 'v1.0.0'.encode('UTF-8')
    job_name = 'visits_reducer'.encode('UTF-8')

    reduced_visit_schema = 'visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING'
    visits_from_kafka = (input_data_stream
                         .select('key',
                                 F.from_json(F.col('value').cast('string'), reduced_visit_schema).alias('visit'),
                                 'headers')
                         .select('key',
                                 F.to_json(F.col('visit')).alias('value'),
                                 F.transform(F.col('headers'), lambda binary_text:  binary_text.cast('STRING')).alias('headers')))


    def write_reduced_visits_to_kafka(visits_to_save: DataFrame, batch_number: int):
        visits_with_metadata = (visits_to_save.withColumn('headers', F.array(
            F.struct(F.lit('job_name').alias('key'), F.lit(job_name).alias('value')),
            F.struct(F.lit('job_version').alias('key'), F.lit(job_version).alias('value')),
            F.struct(F.lit('batch_version').alias('key'), F.lit(str(batch_number).encode('UTF-8')).alias('value')),
            F.struct(F.lit('parent_lineage').alias('key'), F.to_json(F.col('headers')).cast('binary').alias('value'))
        )))

        (visits_with_metadata.write.format('kafka')
         .option('kafka.bootstrap.servers', 'localhost:9094')
         .option('includeHeaders', True)
         .option('topic', 'visits-reduced')
         .save())

    write_data_stream = (visits_from_kafka.writeStream
                         .option('checkpointLocation', f'{get_base_dir()}/checkpoint-reduced2143')
                         .foreachBatch(write_reduced_visits_to_kafka))

    write_data_stream.start().awaitTermination()

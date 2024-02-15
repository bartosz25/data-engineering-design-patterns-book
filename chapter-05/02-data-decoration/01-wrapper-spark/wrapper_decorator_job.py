from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, TimestampType, StringType

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

    job_version = 'v1.0.3'

    visits_from_kafka = input_data_stream.select('key', 'value')


    def write_decorated_visits_to_kafka(visits_to_save: DataFrame, batch_number: int):
        visits_with_processing_context = (visits_to_save.withColumn('processing_context', F.struct(
            F.lit(job_version).alias('job_version'),
            F.lit(batch_number).alias('batch_version')
        )))

        visits_to_save = (visits_with_processing_context.withColumn('value', F.to_json(
            F.struct(
                F.col('value').cast('string').alias('raw_data'),
                F.col('processing_context')
            )
        )))

        (visits_to_save.write.format('kafka')
         .option('kafka.bootstrap.servers', 'localhost:9094')
         .option('topic', 'visits-decorated')
         .save())


    write_data_stream = (visits_from_kafka.writeStream
                         .option('checkpointLocation', '/tmp/dedp/ch05/decorator/visits-wrapper/checkpoint')
                         .foreachBatch(write_decorated_visits_to_kafka))

    write_data_stream.start().awaitTermination()

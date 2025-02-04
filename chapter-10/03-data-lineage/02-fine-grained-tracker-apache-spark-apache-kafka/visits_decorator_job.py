from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StructType, StructField, TimestampType, StringType

from config import get_base_dir

if __name__ == '__main__':
    spark = (SparkSession.builder.master('local[*]')
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') 
        .getOrCreate())

    input_data_stream = (spark.readStream
        .option('kafka.bootstrap.servers', 'localhost:9094') 
        .option('subscribe', 'visits') 
        .option('includeHeaders', 'true') 
        .option('startingOffsets', 'EARLIEST') 
        .format('kafka').load())

    job_version = 'v1.0.0'.encode('UTF-8')
    job_name = 'visits_decorator'.encode('UTF-8')

    visits_from_kafka = input_data_stream.select('key', 'value')

    def write_decorated_visits_to_kafka(visits_to_save: DataFrame, batch_number: int):
        visits_with_metadata = (visits_to_save.withColumn('headers', F.array(
            F.struct(F.lit('job_version').alias('key'), F.lit(job_version).alias('value')),
            F.struct(F.lit('job_name').alias('key'), F.lit(job_name).alias('value')),
            F.struct(F.lit('batch_version').alias('key'), F.lit(str(batch_number).encode('UTF-8')).alias('value'))
        )))

        (visits_with_metadata.write.format('kafka')
         .option('kafka.bootstrap.servers', 'localhost:9094')
         .option('includeHeaders', True)
         .option('topic', 'visits-decorated')
         .save())

    write_data_stream = (visits_from_kafka.writeStream
                         .option('checkpointLocation', f'{get_base_dir()}/checkpoint-decorated')
                         .foreachBatch(write_decorated_visits_to_kafka))

    write_data_stream.start().awaitTermination()

from pyspark.sql import functions as F

from config import get_base_dir
from spark_session_factory import create_spark_session_with_open_lineage

if __name__ == '__main__':
    spark = create_spark_session_with_open_lineage('visits_reducer')

    input_data_stream = (spark.readStream
        .option('kafka.bootstrap.servers', 'localhost:9094')
        .option('subscribe', 'visits-processed')
        .option('includeHeaders', 'true')
        .option('startingOffsets', 'EARLIEST')
        .format('kafka').load())

    job_version = 'v1.0.0'.encode('UTF-8')
    job_name = 'visits_reducer'.encode('UTF-8')

    reduced_visit_schema = 'visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING'
    visits_from_kafka = (input_data_stream
                         .select('key', F.from_json(F.col('value').cast('string'), reduced_visit_schema).alias('visit'))
                         .select('key', F.to_json(F.col('visit')).alias('value')))

    visits_with_metadata = (visits_from_kafka.withColumn('headers', F.array(
        F.struct(F.lit('job_name').alias('key'), F.lit(job_name).alias('value')),
        F.struct(F.lit('job_version').alias('key'), F.lit(job_version).alias('value'))
    )))


    write_data_stream = (visits_with_metadata.writeStream
                         .format('kafka').option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('topic', 'visits-reduced')
                         .option('includeHeaders', True)
                         .option('checkpointLocation', f'{get_base_dir()}/checkpoint-reduced'))

    write_data_stream.start().awaitTermination()

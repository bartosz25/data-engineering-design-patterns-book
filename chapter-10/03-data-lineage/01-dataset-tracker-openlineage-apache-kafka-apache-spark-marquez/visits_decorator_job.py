from pyspark.sql import functions as F

from config import get_base_dir
from spark_session_factory import create_spark_session_with_open_lineage

if __name__ == '__main__':
    spark = create_spark_session_with_open_lineage('visits_decorator')

    input_data_stream = (spark.readStream
        .option('kafka.bootstrap.servers', 'localhost:9094') 
        .option('subscribe', 'visits') 
        .option('includeHeaders', 'true') 
        .option('startingOffsets', 'EARLIEST') 
        .format('kafka').load())

    job_version = 'v1.0.0'.encode('UTF-8')
    job_name = 'visits_decorator'.encode('UTF-8')

    visits_from_kafka = (input_data_stream.select('key', 'value').withColumn('headers', F.array(
            F.struct(F.lit('job_version').alias('key'), F.lit(job_version).alias('value')),
            F.struct(F.lit('job_name').alias('key'), F.lit(job_name).alias('value'))
        )))


    write_data_stream = (visits_from_kafka.writeStream
                         .format('kafka').option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('includeHeaders', True).option('topic', 'visits-processed')
                         .option('checkpointLocation', f'{get_base_dir()}/checkpoint-decorated')
                     )

    write_data_stream.start().awaitTermination()

from pyspark.sql import SparkSession, functions as F, DataFrame
from pyspark.sql.types import StringType

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

    visit_schema = 'user_id STRING'
    visits_from_kafka = (input_data_stream
                         .select(F.from_json(F.col('value').cast('string'), visit_schema).alias('visit'))
                         .select('visit', 'key', 'value')
                         )
    user_to_delete = ''
    rows_to_delete = visits_from_kafka.filter(f'visit.user_id = "{user_to_delete}"')

    rows_to_overwrite = rows_to_delete.withColumn('value', F.lit(None).cast(StringType()))

    write_data_stream = (rows_to_overwrite.writeStream.format('kafka')
     .option('kafka.bootstrap.servers', 'localhost:9094')
     .option('topic', 'visits')
     .option('checkpointLocation', f'{get_base_dir()}/checkpoint')
     )

    write_data_stream.start().awaitTermination()

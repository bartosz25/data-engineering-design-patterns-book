from pyspark.sql import SparkSession, functions as F, DataFrame

spark_session = SparkSession.builder.master('local[*]') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
    .config('spark.sql.shuffle.partitions', 2).getOrCreate()

input_stream_data = spark_session.readStream.format('kafka').option('kafka.bootstrap.servers', 'localhost:9094') \
    .option('subscribe', 'visits') \
    .option('startingOffsets', 'EARLIEST') \
    .load() \
    .selectExpr('CAST(value AS STRING) AS value') \

base_dir = '/tmp/dedp/ch03/fault-tolerance/micro-batch'


def synchronize_visits_to_files(visits_dataframe: DataFrame, batch_number: int):
    visits_dataframe.write.mode('append').text(f'{base_dir}/data')


write_query = (input_stream_data.writeStream.outputMode('update')
               .option('checkpointLocation', f'{base_dir}/checkpoint')
               .foreachBatch(synchronize_visits_to_files).start())

write_query.awaitTermination()

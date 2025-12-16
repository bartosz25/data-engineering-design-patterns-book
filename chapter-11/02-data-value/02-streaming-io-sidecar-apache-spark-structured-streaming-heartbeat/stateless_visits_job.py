from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import get_base_dir

if __name__ == '__main__':
    spark = configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                           .config("spark.sql.extensions",
                                                   "io.delta.sql.DeltaSparkSessionExtension")
                                           .config("spark.sql.catalog.spark_catalog",
                                                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                           extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                           ).getOrCreate()

    heartbeat_data_stream = spark.readStream.format('rate-micro-batch').options(
        rowsPerBatch=1, numPartitions=1
    ).load().filter('false').selectExpr('"" AS value')

    input_data_stream = (spark.readStream
        .option('kafka.bootstrap.servers', 'localhost:9094')
        .option('subscribe', 'visits')
        .option('startingOffsets', 'EARLIEST')
        .format('kafka').load())

    data_to_process = input_data_stream.unionByName(heartbeat_data_stream, allowMissingColumns=True)

    visit_schema = 'event_id STRING, visit_id LONG, event_time TIMESTAMP, page STRING'
    visits_to_delay: DataFrame = (data_to_process
                                   .select(F.col('value').cast('string'))
                                   .select(F.from_json('value', visit_schema).alias('value'))
                                   .selectExpr('value.*')
                                   )

    def dispatch_delayed_visits(new_visits: DataFrame, batch_number: int):
        flush_micro_batch_number = batch_number + 2
        (new_visits
         .withColumn('saving_time', F.current_timestamp())
         .withColumn('flush_number', F.lit(flush_micro_batch_number)).write
         .format('delta').mode('append').saveAsTable('visits_to_flush'))

        visits_to_write = (new_visits.sparkSession.read.table('visits_to_flush')
                           .filter(f'flush_number = {batch_number}'))
        new_visits.sparkSession.read.table('visits_to_flush').show(truncate=False)
        (visits_to_write
         .withColumn('delivery_time', F.current_timestamp())
         .withColumn('delivery_batch', F.lit(batch_number))
         .withColumn('value', F.to_json(F.struct('*'))).select('value').write
         .format('kafka').option('topic', 'visits_output').option('kafka.bootstrap.servers', 'localhost:9094')
         .save())

        new_visits.sparkSession.sql(f'DELETE FROM visits_to_flush WHERE flush_number = {batch_number}')


    write_data_stream = (visits_to_delay.writeStream
                         .trigger(processingTime='15 seconds')
                         .option('checkpointLocation', f'{get_base_dir()}/checkpoint')
                         .foreachBatch(dispatch_delayed_visits)
                         )

    write_data_stream.start().awaitTermination()

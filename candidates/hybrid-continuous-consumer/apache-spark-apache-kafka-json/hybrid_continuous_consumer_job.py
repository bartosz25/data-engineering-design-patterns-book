from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from historical_data_loader import load_historical_data
from real_time_data_loader import load_real_time_data

if __name__ == '__main__':
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master('local[*]')
                                                    .config('spark.sql.extensions',
                                                            'io.delta.sql.DeltaSparkSessionExtension')
                                                    .config('spark.sql.catalog.spark_catalog',
                                                            'org.apache.spark.sql.delta.catalog.DeltaCatalog'),
                                                    extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                                    ).getOrCreate())


    visits_from_json_files = load_historical_data(spark_session).withColumn('origin', F.lit('batch'))
    real_time_visits_from_kafka = load_real_time_data(spark_session).withColumn('origin', F.lit('real_time'))

    visit_to_write = (visits_from_json_files.unionByName(real_time_visits_from_kafka,
                                                        allowMissingColumns=False)
                      .withColumn('processing_time', F.current_timestamp()))

    # Let's trigger it with the available now trigger so that we can see a single file processed in the micro-batch
    streaming_query = (visit_to_write.writeStream.trigger(availableNow=True)
                       .option('checkpointLocation', '/tmp/dedp/candidates/hybrid-continuous-consumer/apache-spark/checkpoint')
     .format('delta').start('/tmp/dedp/candidates/hybrid-continuous-consumer/apache-spark/delta_table'))

    streaming_query.awaitTermination()

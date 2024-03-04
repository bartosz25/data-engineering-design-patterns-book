from pyspark.sql import SparkSession, functions as F, DataFrame

from kafka_writer_with_enricher import KafkaWriterWithEnricher

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[*]') \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
        .getOrCreate()

    input_data_stream = spark.readStream \
        .option('kafka.bootstrap.servers', 'localhost:9094') \
        .option('subscribe', 'visits') \
        .option('startingOffsets', 'LATEST') \
        .format('kafka').load()

    visit_schema = 'visit_id STRING, context STRUCT<user STRUCT<ip STRING>>'

    visits_from_kafka: DataFrame = (input_data_stream
                                    .select(F.from_json(F.col('value').cast('string'), visit_schema).alias('visit'),
                                            'value')
                                    .selectExpr('visit.visit_id', 'visit.context.user.ip', 'CAST(value AS STRING)'))

    write_data_stream = (visits_from_kafka.writeStream
                         .trigger(processingTime='20 seconds')  # 20 seconds to get as many events for a visit at once
                         .option('checkpointLocation',
                                 '/tmp/dedp/ch05/01-data-enrichment/01-static-joiner-api/checkpoint')
                         .foreach(KafkaWriterWithEnricher(bootstrap_server='localhost:9094',
                                                          output_topic='visits-enriched')))

    write_data_stream.start().awaitTermination()

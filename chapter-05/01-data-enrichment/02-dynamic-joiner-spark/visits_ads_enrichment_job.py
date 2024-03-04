from pyspark.sql import SparkSession, functions as F, DataFrame

if __name__ == '__main__':
    spark_session = (SparkSession.builder.master("local[*]")
                     .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
                     .config("spark.sql.shuffle.partitions", 2).getOrCreate())

    visits_data_stream = (spark_session.readStream
                          .option('kafka.bootstrap.servers', 'localhost:9094')
                          .option('subscribe', 'visits')
                          .option('startingOffsets', 'LATEST')
                          .format('kafka').load())

    ads_data_stream = (spark_session.readStream
                       .option('kafka.bootstrap.servers', 'localhost:9094')
                       .option('subscribe', 'ads')
                       .option('startingOffsets', 'LATEST')
                       .format('kafka').load())

    visit_schema = 'visit_id STRING, event_time TIMESTAMP, page STRING'
    visits_from_kafka: DataFrame = (visits_data_stream
                                    .select(F.from_json(F.col('value').cast('string'), visit_schema).alias('visit'))
                                    .selectExpr('visit.*')
                                    .withColumnRenamed('page', 'visit_page')
                                    .withWatermark('event_time', '10 minutes'))

    ad_schema = 'page STRING, display_time TIMESTAMP, campaign_name STRING'
    ads_from_kafka: DataFrame = (ads_data_stream
                                 .select(F.from_json(F.col('value').cast('string'), ad_schema).alias('ad'))
                                 .selectExpr('ad.*')
                                 .withWatermark('display_time', '10 minutes'))

    visits_with_ads = visits_from_kafka.join(ads_from_kafka, F.expr('''
        page = visit_page AND display_time BETWEEN event_time AND event_time + INTERVAL 2 minutes
    '''), 'left_outer').selectExpr('TO_JSON(STRUCT(*)) AS value')

    write_query = (visits_with_ads.writeStream.outputMode("append")
                   .option("checkpointLocation", '/tmp/dedp/chapter-05/01-data-enrichment/02-dynamic-joiner-spark')
                   .format("kafka")
                   .option('kafka.bootstrap.servers', 'localhost:9094')
                   .option('topic', 'visits-enriched')
                   .start())

    write_query.awaitTermination()

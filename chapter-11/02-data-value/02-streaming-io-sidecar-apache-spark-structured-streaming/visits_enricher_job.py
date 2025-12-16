from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import get_base_dir, get_devices_table_dir
from visits_writer import write_enriched_visits

if __name__ == '__main__':
    spark = configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                           .config("spark.sql.extensions",
                                                   "io.delta.sql.DeltaSparkSessionExtension")
                                           .config("spark.sql.catalog.spark_catalog",
                                                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                           extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0']
                                           ).getOrCreate()

    input_data_stream = (spark.readStream 
        .option('kafka.bootstrap.servers', 'localhost:9094') 
        .option('subscribe', 'visits') 
        .option('startingOffsets', 'LATEST') 
        .format('kafka').load())

    visit_schema = '''
        visit_id STRING, event_time TIMESTAMP, user_id STRING, page STRING,
        context STRUCT<
            referral STRING, ad_id STRING, 
            user STRUCT<
                ip STRING, login STRING, connected_since TIMESTAMP
            >,
            technical STRUCT<
                browser STRING, browser_version STRING, network_type STRING, device_type STRING, device_version STRING
            >
        >
    '''
    visits_to_enrich: DataFrame = (input_data_stream
                                   .select(F.col('value').cast('string'))
                                   .select(F.from_json('value', visit_schema).alias('value'))
                                   .selectExpr('value.*')
                                   )

    devices_table: DataFrame = spark.read.format('delta').load(get_devices_table_dir())

    enriched_visits = (visits_to_enrich.join(devices_table,
                                             [visits_to_enrich.context.technical.device_type == devices_table.type,
                                              visits_to_enrich.context.technical.device_version == devices_table.version
                                              ],
                                             'left_outer')
                       )

    write_data_stream = (enriched_visits.writeStream
                         .option('checkpointLocation', f'{get_base_dir()}/checkpoint')
                         .foreachBatch(write_enriched_visits)
                         )

    write_data_stream.start().awaitTermination()

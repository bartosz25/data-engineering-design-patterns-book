from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, DataFrame

from config import get_base_dir, get_devices_table_dir

if __name__ == '__main__':
    spark = configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                           .config("spark.sql.extensions",
                                                   "io.delta.sql.DeltaSparkSessionExtension")
                                           .config("spark.sql.catalog.spark_catalog",
                                                   "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
                                           extra_packages=['org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0']
                                           ).getOrCreate()

    input_data_stream = spark.readStream \
        .option('kafka.bootstrap.servers', 'localhost:9094') \
        .option('subscribe', 'visits') \
        .option('startingOffsets', 'LATEST') \
        .format('kafka').load()

    visit_schema_for_enrichment = '''
        context STRUCT<
            technical STRUCT<
                device_type STRING, device_version STRING
            >
        >
    '''

    visits_to_enrich: DataFrame = (input_data_stream
                                   .select(F.col('value').cast('string'))
                                   .select(F.from_json('value', visit_schema_for_enrichment).alias('technical'),
                                           'value')
                                   .select('technical.context.technical.device_type',
                                           'technical.context.technical.device_version',
                                           'value')
                                   )

    devices_table: DataFrame = spark.read.format('delta').load(get_devices_table_dir())

    enriched_visits = (visits_to_enrich.join(devices_table,
                                             [visits_to_enrich.device_type == devices_table.type,
                                              visits_to_enrich.device_version == devices_table.version
                                              ],
                                             'left_outer')
                       )

    visits_to_write = (enriched_visits
    .select(F.to_json(
        F.struct(
            F.col('value').alias('visit'),
            F.struct(F.col('version'), F.col('type'), F.col('full_name')).alias('device')
        )
    ).alias('value')))

    write_data_stream = (visits_to_write.writeStream
                         .option('checkpointLocation', f'{get_base_dir()}/checkpoint')
                         .format('kafka')
                         .option('kafka.bootstrap.servers', 'localhost:9094')
                         .option('topic', 'visits_enriched')
                         )

    write_data_stream.start().awaitTermination()

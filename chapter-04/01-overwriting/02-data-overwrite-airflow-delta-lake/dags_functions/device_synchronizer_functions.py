def create_tables_if_needed():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.catalogImplementation", "hive")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    spark_session.sql(f'''
              CREATE TABLE IF NOT EXISTS `default`.`devices` (
                id STRING NOT NULL,
                brand_name STRING NOT NULL,
                full_name STRING NOT NULL,
                processor_brand STRING NOT NULL,
                execution_time STRING NOT NULL
              ) USING DELTA
            ''')


def write_new_devices(currently_processed_execution_date, base_dir):
    from delta import configure_spark_with_delta_pip, DeltaTable
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.catalogImplementation", "hive")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                   ).getOrCreate())
    new_devices = (
        spark_session.read.schema('id STRING, brand_name STRING, full_name STRING, processor_brand STRING')
        .json(f'{base_dir}/{currently_processed_execution_date}.json').alias('new_devices'))

    (new_devices.withColumn('execution_time', F.lit(currently_processed_execution_date))
     .write.format('delta')
     .option('replaceWhere', f'execution_time = "{currently_processed_execution_date}"')
     .mode('overwrite').saveAsTable('default.devices')
     )
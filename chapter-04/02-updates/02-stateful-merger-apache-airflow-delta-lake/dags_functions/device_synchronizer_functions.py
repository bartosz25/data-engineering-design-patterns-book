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
              CREATE TABLE IF NOT EXISTS `default`.`versions` (
                execution_time STRING NOT NULL,
                delta_table_version INT NOT NULL
              ) USING DELTA
            ''')

    spark_session.sql(f'''
              CREATE TABLE IF NOT EXISTS `default`.`devices` (
                id STRING NOT NULL,
                brand_name STRING NOT NULL,
                full_name STRING NOT NULL,
                processor_brand STRING NOT NULL
              ) USING DELTA
            ''')

def restore_table_if_needed(previous_execution_date,
                            currently_processed_execution_date):
    from delta import configure_spark_with_delta_pip, DeltaTable
    from pyspark.sql import SparkSession
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                    .config("spark.sql.catalogImplementation", "hive")
                                                    .config("spark.sql.extensions",
                                                            "io.delta.sql.DeltaSparkSessionExtension")
                                                    .config("spark.sql.catalog.spark_catalog",
                                                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                    ).getOrCreate())

    versions_history = spark_session.sql('DESCRIBE HISTORY default.devices')
    versions_history.cache()

    last_merge_version = (versions_history.filter('operation = "MERGE"')
                          .selectExpr('MAX(version) AS last_version').collect()[0].last_version)
    print(f'Last merge version was {last_merge_version}')

    maybe_previous_job_version = spark_session.sql(
        f'SELECT delta_table_version FROM versions WHERE execution_time = "{previous_execution_date}"').collect()
    if maybe_previous_job_version:
        previous_job_version = maybe_previous_job_version[0].delta_table_version
        if previous_job_version == last_merge_version:
            print(
                f"Nothing to restore: previous job's version {previous_job_version} vs. last merge version {last_merge_version}")
        else:
            current_run_version = spark_session.sql(
                f'SELECT delta_table_version FROM versions WHERE execution_time = "{currently_processed_execution_date}"').collect()[
                0].delta_table_version
            version_to_restore = current_run_version - 1
            print(f'Restoring to {version_to_restore}')
            (DeltaTable.forName(spark_session, 'devices').restoreToVersion(version_to_restore ))
    else:
        print('Missing previous version, truncating the table')
        # local SparkSession doesn't support TRUNCATE; we run DELETE instead even though you should prefer TRUNCATE
        # which is faster
        spark_session.sql('DELETE FROM default.devices')

    versions_history.unpersist()


def merge_datasets(currently_processed_execution_date, base_dir):
    from delta import configure_spark_with_delta_pip, DeltaTable
    from pyspark.sql import SparkSession
    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                   .config("spark.sql.catalogImplementation", "hive")
                                                   .config("spark.sql.extensions",
                                                           "io.delta.sql.DeltaSparkSessionExtension")
                                                   .config("spark.sql.catalog.spark_catalog",
                                                           "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                   ).getOrCreate())
    new_devices = (
        spark_session.read.schema('id STRING, brand_name STRING, full_name STRING NOT NULL, processor_brand STRING')
        .json(f'{base_dir}/{currently_processed_execution_date}.json').alias('new_devices'))

    (DeltaTable.forName(spark_session, 'devices').alias('base_table')
     .merge(new_devices.alias('new_table'), 'base_table.id = new_table.id')
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

def update_state_table(currently_processed_execution_date):
    from delta import configure_spark_with_delta_pip, DeltaTable
    from pyspark.sql import SparkSession
    from pyspark import Row

    spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                        .config("spark.sql.catalogImplementation", "hive")
                                                        .config("spark.sql.extensions",
                                                                "io.delta.sql.DeltaSparkSessionExtension")
                                                        .config("spark.sql.catalog.spark_catalog",
                                                                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                        ).getOrCreate())
    last_written_version_after_data_insert = (spark_session.sql('DESCRIBE HISTORY default.devices')
            .selectExpr('MAX(version) AS last_version').collect()[0].last_version)
    new_version = (spark_session
                   .createDataFrame([Row(execution_time=currently_processed_execution_date,
                                         delta_table_version=last_written_version_after_data_insert)]))
    (DeltaTable.forName(spark_session, 'versions').alias('old_versions')
     .merge(new_version.alias('new_version'), 'old_versions.execution_time = new_version.execution_time')
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
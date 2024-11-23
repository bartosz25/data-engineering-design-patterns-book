import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

with (DAG('devices_synchronizer', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.parse("2024-09-20"),
             'end_date': pendulum.parse("2024-09-24"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval='@daily', catchup=True) as dag):
    BASE_DIR = '/tmp/dedp/ch04/02-updates/01-merger-apache-airflow-delta-lake-state-table/input'
    next_file_path = BASE_DIR + '/{{ logical_date | ds_nodash }}.json'
    wait_for_the_input_file = FileSensor(
        task_id='wait_for_the_input_file',
        filepath=next_file_path,
        mode='reschedule',
    )

    @task.virtualenv(
        task_id="create_tables_if_needed", requirements=["delta-spark==3.0.0", "pyspark==3.5.0"], system_site_packages=False
    )
    def create_tables_if_needed():
        # typically this can be managed externally, such as from a schema management CI/CD pipeline
        # but for the sake of simplicity, we keep this part in the DAG itself
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
            job_version STRING NOT NULL,
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


    @task.virtualenv(
        task_id="update_output_table", requirements=["delta-spark==3.0.0", "pyspark==3.5.0"], system_site_packages=False,
    )
    def update_output_table(previous_execution_date, current_execution_date, base_dir):
        from delta import configure_spark_with_delta_pip, DeltaTable
        from pyspark.sql import SparkSession
        from pyspark import Row

        def insert_last_version_for_the_job(job_id: str):
            last_written_version_after_data_insert = (spark_session.sql('DESCRIBE HISTORY default.devices')
                                    .selectExpr('MAX(version) AS last_version').collect()[0].last_version)
            new_version = (spark_session
                           .createDataFrame([Row(job_version=job_id, delta_table_version=last_written_version_after_data_insert)]))
            (DeltaTable.forName(spark_session, 'versions').alias('old_versions')
             .merge(new_version.alias('new_version'), 'old_versions.job_version = new_version.job_version')
             .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

        spark_session = (configure_spark_with_delta_pip(SparkSession.builder.master("local[*]")
                                                        .config("spark.sql.catalogImplementation", "hive")
                                                        .config("spark.sql.extensions",
                                                                "io.delta.sql.DeltaSparkSessionExtension")
                                                        .config("spark.sql.catalog.spark_catalog",
                                                                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                        ).getOrCreate())

        if previous_execution_date:
            last_written_version = (spark_session.sql('DESCRIBE HISTORY default.devices')
                                    .selectExpr('MAX(version) AS last_version').collect()[0].last_version)
            maybe_previous_job_version = spark_session.sql(f'SELECT delta_table_version FROM versions WHERE '
                                                     f'job_version = "{previous_execution_date}"').collect()
            previous_job_version = None
            if maybe_previous_job_version:
                previous_job_version = maybe_previous_job_version[0].delta_table_version

            if not previous_job_version:
                print('Missing previous version, truncating the table')
                # TRUNCATE is not supported in the OS Delta Lake, let's simulate it with a DELETE
                spark_session.sql('DELETE FROM default.devices')
            elif not previous_job_version == last_written_version:
                print(f"Nothing to restore: previous job's version {previous_job_version} vs. last written version {last_written_version}")
            else:
                print(f"Restoring table to {previous_job_version} (current version was {last_written_version})")
                # restore != time travel
                (DeltaTable.forName(spark_session, 'devices').restoreToVersion(previous_job_version ))

        new_devices = (spark_session.read.schema('id STRING, brand_name STRING, full_name STRING NOT NULL, processor_brand STRING')
                       .json(f'{base_dir}/{current_execution_date}.json').alias('new_devices'))

        (DeltaTable.forName(spark_session, 'devices').alias('base_table')
         .merge(new_devices.alias('new_table'), 'base_table.id = new_table.id')
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

        insert_last_version_for_the_job(current_execution_date)


    wait_for_the_input_file >> create_tables_if_needed() >> update_output_table(
        previous_execution_date='{{ prev_data_interval_start_success | ds_nodash }}',
        current_execution_date='{{ logical_date | ds_nodash }}', base_dir=BASE_DIR
    )

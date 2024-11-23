import os
from datetime import timedelta
from typing import Optional

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

with (DAG('devices_reader', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.parse("2024-09-20"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval=None, catchup=False) as dag):
    BASE_DIR = '/tmp/dedp/ch04/02-updates/01-merger-apache-airflow-delta-lake-state-table/input'

    @task.virtualenv(
        task_id="read_devices", requirements=["delta-spark==3.0.0", "pyspark==3.5.0"], system_site_packages=False
    )
    def read_devices():
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

        spark_session.sql('SELECT * FROM `default`.`devices` ORDER BY id ASC').show(truncate=False)


    @task.virtualenv(
        task_id="read_versions", requirements=["delta-spark==3.0.0", "pyspark==3.5.0"], system_site_packages=False
    )
    def read_versions():
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

        spark_session.sql('SELECT * FROM `default`.`versions` ORDER BY job_version ASC').show(truncate=False)

    read_devices()

    read_versions()
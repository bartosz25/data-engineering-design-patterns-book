import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago

from macros import get_table_name

with DAG('devices_loader', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': days_ago(2),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         is_paused_upon_creation=True,
         catchup=False,
         user_defined_macros={'get_table_name': get_table_name},
         template_searchpath=[os.getcwd()],
         schedule_interval='@daily',
         ) as dag:
    input_devices_file = '/tmp/dedp/ch10/03-data-lineage/01-dataset-tracker-openlineage-apache-airflow-marquez/input/dataset.csv'

    input_data_sensor = FileSensor(
        task_id='input_data_sensor',
        filepath=input_devices_file,
        mode='reschedule'
    )

    load_data_to_table = PostgresOperator(
        task_id='load_data_to_table',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/load_file_to_device_table.sql'
    )


    success_execution_marker = ExternalTaskMarker(
        task_id='trigger_downstream_consumers',
        external_dag_id='devices_aggregator',
        external_task_id='downstream_trigger_sensor',
    )

    input_data_sensor >> load_data_to_table >> success_execution_marker

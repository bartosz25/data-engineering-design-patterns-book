import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

from macros import get_internal_base_location_for_devices_file

with DAG('devices_aggregator', max_active_runs=1,
         default_args={
             'depend_on_past': True,
             'start_date': pendulum.now(tz='utc').subtract(days=4),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily", catchup=True):

    input_data_sensor = FileSensor(
        task_id='input_data_sensor',
        filepath=get_internal_base_location_for_devices_file() + '/{{ ds_nodash }}/dataset.csv',
        poke_interval=30,
        mode='reschedule',
        do_xcom_push=False
    )

    database_schema = 'dedp'
    load_data_to_table = PostgresOperator(
        task_id='load_data_to_table',
        postgres_conn_id='docker_postgresql',
        database=database_schema,
        sql='/sql/load_file_to_device_table.sql'
    )

    refresh_aggregates = PostgresOperator(
        task_id='refresh_aggregates',
        postgres_conn_id='docker_postgresql',
        database=database_schema,
        sql='/sql/refresh_aggregates.sql'
    )

    input_data_sensor >> load_data_to_table >> refresh_aggregates


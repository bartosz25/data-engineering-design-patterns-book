import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

from macros import get_input_csv_to_load_for_host, get_table_name

with DAG('devices_loader', max_active_runs=1,
         default_args={
             'depend_on_past': True,
             'start_date': pendulum.now(tz='utc').subtract(days=4),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         user_defined_macros={'get_table_name': get_table_name,
                              'get_input_csv_to_load': get_input_csv_to_load_for_host},
         schedule_interval="@daily") as dag:
    input_devices_file = get_input_csv_to_load_for_host()
    database_schema = 'dedp_test'

    input_data_sensor = FileSensor(
        task_id='input_data_sensor',
        filepath=input_devices_file,
        mode='reschedule',
        do_xcom_push=False
    )

    load_data_to_table = PostgresOperator(
        task_id='load_data_to_table',
        postgres_conn_id='docker_postgresql',
        database=database_schema,
        sql='/sql/load_file_to_device_table.sql'
    )

    expose_new_table = PostgresOperator(
        task_id='load_data_to_the_final_table',
        postgres_conn_id='docker_postgresql',
        database=database_schema,
        sql='/sql/expose_new_table.sql'
    )

    input_data_sensor >> load_data_to_table >> expose_new_table

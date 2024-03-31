import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from config import get_data_location_base_dir, get_database_final_schema
from macros import get_input_csv_to_load, get_devices_table_name

with DAG('devices_loader', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.datetime(2023, 8, 28),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         user_defined_macros={'get_devices_table_name': get_devices_table_name,
                              'get_input_csv_to_load': get_input_csv_to_load},
         schedule_interval='@once', catchup=False) as dag:
    input_data_file_path = get_data_location_base_dir(False) + '/dataset.csv'
    base_table_name = 'devices'

    load_data_to_internal_table = PostgresOperator(
        task_id='load_data_to_internal_table',
        postgres_conn_id='docker_postgresql',
        database=get_database_final_schema(),
        params={'input_data_path': get_data_location_base_dir(True) + '/dataset.csv'},
        sql='/sql/load_visits_to_weekly_table.sql'
    )

    refresh_view = PostgresOperator(
        task_id='refresh_view',
        postgres_conn_id='docker_postgresql',
        database=get_database_final_schema(),
        sql='/sql/refresh_view.sql'
    )

    load_data_to_internal_table >> refresh_view

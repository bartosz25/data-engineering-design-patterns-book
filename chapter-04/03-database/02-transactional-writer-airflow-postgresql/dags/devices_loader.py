import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

with DAG('devices_loader', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.datetime(2023, 8, 28),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@once", catchup=True) as dag:

    load_new_devices = PostgresOperator(
        task_id='load_new_devices',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/load_new_devices.sql'
    )

    load_new_devices

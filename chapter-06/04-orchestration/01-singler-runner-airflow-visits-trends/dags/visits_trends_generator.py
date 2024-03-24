import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

with DAG('visits_trend_generator', max_active_runs=1,
         default_args={
             'depend_on_past': True,
             'start_date': pendulum.datetime(2024, 2, 1),
             'end_date': pendulum.datetime(2024, 2, 5),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily", catchup=True) as dag:

    load_visits = PostgresOperator(
        task_id='load_visits',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/load_visits.sql'
    )

    generate_trends = PostgresOperator(
        task_id='generate_trends',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/generate_trends.sql'
    )

    load_visits >> generate_trends

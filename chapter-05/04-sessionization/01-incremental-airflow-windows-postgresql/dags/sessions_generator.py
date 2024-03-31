import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

with DAG('sessions_generator', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.datetime(2024, 2, 1),
             'end_date': pendulum.datetime(2024, 2, 5),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily", catchup=True) as dag:

    clean_previous_runs_sessions = PostgresOperator(
        task_id='clean_previous_runs_sessions',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/clean_previous_run_generated_sessions.sql'
    )

    clean_previous_runs_pending_sessions = PostgresOperator(
        task_id='clean_previous_runs_pending_sessions',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/clean_previous_run_pending_sessions.sql'
    )

    generate_sessions = PostgresOperator(
        task_id='generate_sessions',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/generate_sessions.sql'
    )

    [clean_previous_runs_sessions, clean_previous_runs_pending_sessions] >> generate_sessions

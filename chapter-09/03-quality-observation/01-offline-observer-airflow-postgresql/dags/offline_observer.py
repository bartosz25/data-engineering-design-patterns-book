import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

with DAG('offline_observer', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.yesterday(),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval=timedelta(minutes=4), catchup=True) as dag:

    wait_for_new_data = SqlSensor(
        task_id='wait_for_new_data',
        conn_id='docker_postgresql',
        sql='/sql/wait_for_new_data_to_observe.sql',
        mode='reschedule'
    )

    record_new_observation_state = PostgresOperator(
        task_id='record_new_observation_state',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/record_new_observation_state.sql'
    )

    insert_new_observations = PostgresOperator(
        task_id='insert_new_observations',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/insert_new_observations.sql'
    )

    wait_for_new_data >> record_new_observation_state >> insert_new_observations

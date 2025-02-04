import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG('passthrough_visits_synchronizer', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.yesterday(),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval=timedelta(minutes=2), catchup=True) as dag:

    wait_for_new_data = SqlSensor(
        task_id='wait_for_new_data',
        conn_id='docker_postgresql',
        sql='/sql/wait_for_new_data.sql',
        mode='reschedule'
    )

    record_new_synchronization_state = PostgresOperator(
        task_id='record_new_synchronization_state.sql',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/record_new_synchronization_state.sql'
    )

    clean_previously_added_visits = PostgresOperator(
        task_id='clean_previously_added_visits',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/clean_previously_inserted_visits.sql'
    )

    copy_new_visits = PostgresOperator(
        task_id='copy_new_visits',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/copy_new_visits.sql'
    )

    wait_for_new_data >> record_new_synchronization_state >> clean_previously_added_visits >> copy_new_visits

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

from macros import get_table_name

with DAG('devices_aggregator_bi_team', max_active_runs=1,
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

    parent_dag_sensor = ExternalTaskSensor(
        task_id='downstream_trigger_sensor',
        external_dag_id='devices_aggregator',
        external_task_id='trigger_downstream_consumers',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule'
    )
    refresh_bi_aggregates = PostgresOperator(
        task_id='refresh_bi_aggregates',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/refresh_bi_aggregates.sql'
    )

    parent_dag_sensor >> refresh_bi_aggregates

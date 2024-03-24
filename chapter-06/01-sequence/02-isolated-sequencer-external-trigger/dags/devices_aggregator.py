import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor

from macros import get_table_name

with DAG('devices_aggregator', max_active_runs=1,
         default_args={
             'depend_on_past': True,
             'start_date': pendulum.now(tz='utc').subtract(days=4),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         user_defined_macros={'get_table_name': get_table_name},
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily", catchup=True):

    parent_dag_sensor = ExternalTaskSensor(
        task_id='downstream_trigger_sensor',
        external_dag_id='devices_loader',
        external_task_id='trigger_downstream_consumers',
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode="reschedule"
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

    parent_dag_sensor >> load_data_to_table >> refresh_aggregates


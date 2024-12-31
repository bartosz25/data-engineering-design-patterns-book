import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import BranchSQLOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

with (DAG('devices_synchronizer', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.parse("2024-09-20"),
             'end_date': pendulum.parse("2024-09-24"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval='@daily', catchup=True) as dag):

    BASE_DIR = '/tmp/dedp/ch04/02-updates/02-stateful-merger-apache-airflow-postgresql/input'
    next_file_path = BASE_DIR + '/{{ logical_date | ds_nodash }}.csv'
    wait_for_the_input_file = FileSensor(
        task_id='wait_for_the_input_file',
        filepath=next_file_path,
        mode='reschedule',
    )


    should_restore_table = BranchSQLOperator(
        task_id='should_restore_table',
        conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/define_merge_mode.sql',
        follow_task_ids_if_true=['clean_table_before_restore'],
        follow_task_ids_if_false=['noop']
    )

    clean_table_before_restore = PostgresOperator(
        task_id='clean_table_before_restore',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/clean_table_before_restore.sql'
    )

    restore_table = PostgresOperator(
        task_id='restore_table',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/restore_table.sql'
    )

    noop = EmptyOperator(
        task_id='noop'
    )

    load_data_to_historical_table = PostgresOperator(
        task_id='load_data_to_historical_table',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/load_data_to_historical_table.sql',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    merge_new_devices = PostgresOperator(
        task_id='merge_new_devices',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/merge_new_devices.sql'
    )

    wait_for_the_input_file >> should_restore_table
    should_restore_table >> noop >> load_data_to_historical_table >> merge_new_devices
    should_restore_table >> clean_table_before_restore >> restore_table >> load_data_to_historical_table >> merge_new_devices


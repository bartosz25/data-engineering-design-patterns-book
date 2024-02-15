import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule
from operators.view_manager_operator import PostgresViewManagerOperator

from config import get_data_location_base_dir, get_database_final_schema
from macros import get_weekly_table_name, get_input_csv_to_load

with DAG('visits_loader', max_active_runs=1,
         default_args={
             'depend_on_past': True,
             'start_date': pendulum.datetime(2023, 11, 6),
             'end_date': pendulum.datetime(2023, 11, 9),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         user_defined_macros={'get_weekly_table_name': get_weekly_table_name,
                              'get_input_csv_to_load': get_input_csv_to_load},
         schedule_interval="@daily", catchup=True) as dag:
    input_data_file_path = get_data_location_base_dir(False) + '/date={{ ds }}/dataset.csv'
    base_table_name = 'visits_json_copy'

    # This is the Extract part
    input_data_sensor = FileSensor(
        task_id='input_data_sensor',
        filepath=input_data_file_path,
        mode='reschedule',
        do_xcom_push=False
    )

    def retrieve_path_for_table_creation(**context):
        execution_date = context['execution_date']

        should_create_table = execution_date.day_of_week == 1 or execution_date.day_of_year == 1
        return 'create_weekly_table' if should_create_table else "dummy_task"


    check_if_monday_or_first_january_at_midnight = BranchPythonOperator(
        task_id='check_if_monday_or_first_january_at_midnight',
        provide_context=True,
        python_callable=retrieve_path_for_table_creation
    )

    dummy_task = EmptyOperator(
        task_id='dummy_task'
    )

    create_weekly_table = PostgresOperator(
        task_id='create_weekly_table',
        postgres_conn_id='docker_postgresql',
        database=get_database_final_schema(),
        sql='/sql/create_weekly_table.sql'
    )

    recreate_view = PostgresViewManagerOperator(
        task_id='recreate_view',
        postgres_conn_id='docker_postgresql',
        database=get_database_final_schema(),
        view_name='visits',
        schema=get_database_final_schema(),
        sql='/sql/recreate_view.sql'
    )

    load_data_to_the_final_table = SQLExecuteQueryOperator(
        task_id='load_data_to_the_final_table',
        conn_id='docker_postgresql',
        database=get_database_final_schema(),
        trigger_rule=TriggerRule.NONE_FAILED,
        sql='/sql/load_visits_to_weekly_table.sql',
        params={'code_version': 'v1.3.4'}
    )

    input_data_sensor >> check_if_monday_or_first_january_at_midnight \
        >> [dummy_task, create_weekly_table]
    dummy_task >> load_data_to_the_final_table
    create_weekly_table >> recreate_view >> load_data_to_the_final_table

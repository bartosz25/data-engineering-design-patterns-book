import os
from datetime import timedelta

import pandas
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor

from lib.config import get_data_location_base_dir, get_current_partition_file_full_path
from lib.audit_functions import validate_the_file_before_processing, validate_flatten_visits

with DAG('visits_synchronizer', max_active_runs=1,
         default_args={
             'depends_on_past': False,
             'start_date': pendulum.parse("2024-07-01"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval='@daily', catchup=True) as dag:
    next_partition_template = get_data_location_base_dir() + '/date={{ data_interval_end | ds }}'
    current_partition_template = get_data_location_base_dir() + '/date={{ ds }}'

    # This is the Extract part
    next_partition_sensor = FileSensor(
        task_id='next_partition_sensor',
        filepath=next_partition_template,
        mode='reschedule',
        do_xcom_push=False
    )


    def local_validate_the_file_before_processing():
        min_size_10_kb_as_bytes = 10240
        validate_the_file_before_processing(get_current_context(), min_size_10_kb_as_bytes, 40)

    audit_file_to_load = PythonOperator(
        task_id='audit_file_to_load',
        python_callable=local_validate_the_file_before_processing
    )

    def flatten_input_visits_to_csv():
        context = get_current_context()
        visits_to_flatten = pandas.read_json(get_current_partition_file_full_path(context, 'json'), lines=True)
        context_normalized = pandas.json_normalize(visits_to_flatten['context'])
        visits_to_flatten.drop(columns='context', axis=1, inplace=True)

        def remove_prefix(field_to_rename: str) -> str:
            return field_to_rename.split('.')[-1]

        for field_name in context_normalized.columns.values:
            visits_to_flatten[remove_prefix(field_name)] = context_normalized[field_name]

        visits_to_flatten['execution_time'] = context['logical_date']
        print(context)

        visits_to_flatten.to_csv(get_current_partition_file_full_path(context, 'csv'), sep=';', index=False)


    transform_file = PythonOperator(
        task_id='transform_file',
        python_callable=flatten_input_visits_to_csv
    )

    def local_validate_flatten_visits():
        validate_flatten_visits(get_current_context())

    audit_transformed_file = PythonOperator(
        task_id='audit_transformed_file',
        python_callable=local_validate_flatten_visits
    )

    load_flattened_visits_to_final_table = PostgresOperator(
        task_id='load_flattened_visits_to_final_table',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/load_file_to_visits_table.sql'
    )

    next_partition_sensor >> audit_file_to_load >> transform_file >> audit_transformed_file >> load_flattened_visits_to_final_table

import datetime
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import task, get_current_context
from airflow.sensors.filesystem import FileSensor

with DAG('devices_loader', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.parse("2024-09-20"),
             'end_date': pendulum.parse("2024-09-23"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily") as dag:
    dataset_base_dir = '/tmp/dedp/ch03/03-late-data/02-static-late-data-integrator-apache-airflow'
    dataset_input_dir = f'{dataset_base_dir}/dataset/input'
    dataset_output_dir = f'{dataset_base_dir}/dataset/output'

    file_to_load_sensor = FileSensor(
        task_id='file_to_load_sensor',
        filepath=dataset_input_dir + '/{{ data_interval_start | ds }}.json',
        mode='reschedule',
        do_xcom_push=False
    )

    def copy_file(file_name_without_exception: str):
        import shutil
        shutil.copyfile(f'{dataset_input_dir}/{file_name_without_exception}.json', f'{dataset_output_dir}/{file_name_without_exception}.json')

    @task
    def load_current_file():
        context = get_current_context()
        dag_run: DagRun = context['dag_run']
        print(dag_run)
        print(f'Loading late data file {dag_run.execution_date.isoformat()}...')
        date_formatted = dag_run.execution_date.strftime('%Y-%m-%d')
        copy_file(date_formatted)


    @task
    def generate_backfilling_runs():
        context = get_current_context()
        dag_run: DagRun = context['dag_run']
        backfilling_dates = []
        days_to_backfill = 2
        start_date_to_backfill = dag_run.execution_date - datetime.timedelta(days=days_to_backfill)
        for days_to_add in range(0, days_to_backfill):
            date_to_backfill = start_date_to_backfill + datetime.timedelta(days=days_to_add)
            backfilling_dates.append(date_to_backfill.date().strftime('%Y-%m-%d'))

        return backfilling_dates

    @task
    def integrate_late_data(late_date: str):
        print(f'Loading late data file {late_date}...')
        copy_file(late_date)

    backfilling_runs_generator = generate_backfilling_runs()
    file_to_load_sensor >> load_current_file() >> backfilling_runs_generator >> integrate_late_data.expand(late_date=backfilling_runs_generator)

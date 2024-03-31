import json
import shutil
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG('dataset_creator', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.yesterday(tz='utc'),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         schedule_interval="@daily") as dag:

    dataset_dir = '/tmp/dedp/ch02/data-readiness/airflow/dataset'

    @task
    def delete_dataset():
        shutil.rmtree(dataset_dir, ignore_errors=True)

    @task
    def generate_dataset():
        rows = [
            {'browser': 'Firefox', 'version': '22.0'},
            {'browser': 'Chrome', 'version': '10.0'},
            {'browser': 'Safari', 'version': '9.0'}
        ]
        Path(dataset_dir).mkdir(parents=True, exist_ok=True)
        with open(f'{dataset_dir}/dataset.json', 'w') as dataset_file:
            for row in rows:
                dataset_file.write(json.dumps(row))
                dataset_file.write('\n')

    @task
    def create_readiness_file():
        with open(f'{dataset_dir}/COMPLETED', 'w') as marker_file:
            marker_file.write('')


    delete_dataset() >> generate_dataset() >> create_readiness_file()

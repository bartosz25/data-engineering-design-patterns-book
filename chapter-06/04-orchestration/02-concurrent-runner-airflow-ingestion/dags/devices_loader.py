import os
import shutil
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.sensors.filesystem import FileSensor

with DAG('devices_loader', max_active_runs=5,
         default_args={
             'depends_on_past': False,
             'start_date': pendulum.now(tz='utc').subtract(days=4),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         schedule_interval="@daily") as dag:
    input_devices_file = '/tmp/dedp/ch06/04-orchestration/02-concurrent-airflow-data-ingestion/input/dataset.csv'

    input_data_sensor = FileSensor(
        task_id='input_data_sensor',
        filepath=input_devices_file,
        mode='reschedule',
        do_xcom_push=False
    )

    @task
    def load_new_devices_to_internal_storage():
        context = get_current_context()
        internal_base_location = '/tmp/dedp/ch06/04-orchestration/02-concurrent-airflow-data-ingestion/input-internal'
        partitioned_dir = f'{internal_base_location}/{context["ds_nodash"]}'
        shutil.rmtree(partitioned_dir, ignore_errors=True)
        os.mkdir(partitioned_dir)
        internal_file_location = f'{partitioned_dir}/dataset.csv'
        shutil.copyfile(input_devices_file, internal_file_location)

    empty_task_1 = EmptyOperator(
        task_id='empty_task_1'
    )

    empty_task_2 = EmptyOperator(
        task_id='empty_task_2'
    )

    input_data_sensor >> load_new_devices_to_internal_storage() >> empty_task_1 >> empty_task_2

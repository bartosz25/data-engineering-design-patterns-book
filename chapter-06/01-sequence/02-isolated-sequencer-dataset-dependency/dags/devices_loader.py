import os
import shutil
from datetime import timedelta

import shutil
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.sensors.filesystem import FileSensor

from macros import get_internal_base_location_for_devices_file

with DAG('devices_loader', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.now(tz='utc').subtract(days=4),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         schedule_interval="@daily") as dag:
    input_devices_file = '/tmp/dedp/ch06/01-sequence/02-isolated-sequencer-dataset-dependency/input/dataset.csv'

    input_data_sensor = FileSensor(
        task_id='input_data_sensor',
        filepath=input_devices_file,
        mode='reschedule',
        do_xcom_push=False
    )

    @task
    def load_new_devices_to_internal_storage():
        context = get_current_context()
        partitioned_dir = f'{get_internal_base_location_for_devices_file()}/{context["ds_nodash"]}'
        shutil.rmtree(partitioned_dir, ignore_errors=True)
        os.mkdir(partitioned_dir)
        internal_file_location = f'{partitioned_dir}/dataset.csv'
        shutil.copyfile(input_devices_file, internal_file_location)


    input_data_sensor >> load_new_devices_to_internal_storage()

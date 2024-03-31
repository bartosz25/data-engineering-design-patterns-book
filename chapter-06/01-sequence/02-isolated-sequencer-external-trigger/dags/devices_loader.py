import os
import shutil
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskMarker
from airflow.sensors.filesystem import FileSensor

from macros import get_internal_storage_location_for_devices_file

with DAG('devices_loader', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.now(tz='utc').subtract(days=4),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         schedule_interval="@daily") as dag:
    input_devices_file = '/tmp/dedp/ch06/01-sequence/02-isolated-sequencer-external-trigger/input/dataset.csv'

    input_data_sensor = FileSensor(
        task_id='input_data_sensor',
        filepath=input_devices_file,
        mode='reschedule',
        do_xcom_push=False
    )

    @task
    def load_new_devices_to_internal_storage():
        shutil.copyfile(input_devices_file, get_internal_storage_location_for_devices_file())


    success_execution_marker = ExternalTaskMarker(
        task_id='trigger_downstream_consumers',
        external_dag_id='devices_aggregator',
        external_task_id='downstream_trigger_sensor',
    )

    input_data_sensor >> load_new_devices_to_internal_storage() >> success_execution_marker

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

with (DAG('devices_synchronizer', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.parse("2024-09-20"),
             'end_date': pendulum.parse("2024-09-23"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval='@daily', catchup=True) as dag):
    BASE_DIR = '/tmp/dedp/ch04/01-overwriting/02-data-overwrite-airflow-delta-lake/input'
    next_file_path = BASE_DIR + '/{{ logical_date | ds_nodash }}.json'
    wait_for_the_input_file = FileSensor(
        task_id='wait_for_the_input_file',
        filepath=next_file_path,
        mode='reschedule',
    )

    @task.virtualenv(
        task_id="create_tables_if_needed", requirements=["delta-spark==3.0.0", "pyspark==3.5.0"], system_site_packages=False
    )
    def create_tables_if_needed_task():
        from pathlib import Path
        import sys
        sys.path.append(f'{Path.cwd()}/dags_functions')
        from device_synchronizer_functions import create_tables_if_needed
        # typically this can be managed externally, such as from a schema management CI/CD pipeline
        # but for the sake of simplicity, we keep this part in the DAG itself
        create_tables_if_needed()


    @task.virtualenv(
        task_id="write_new_devices", requirements=["delta-spark==3.0.0", "pyspark==3.5.0"],
        system_site_packages=False
    )
    def write_new_devices_task(current_execution_date, base_dir):
        from pathlib import Path
        import sys
        sys.path.append(f'{Path.cwd()}/dags_functions')
        from device_synchronizer_functions import write_new_devices

        write_new_devices(
            currently_processed_execution_date=current_execution_date,
            base_dir=base_dir
        )


    (wait_for_the_input_file >> create_tables_if_needed_task()
     >>
     write_new_devices_task(current_execution_date='{{ logical_date | ds_nodash }}', base_dir=BASE_DIR)
    )
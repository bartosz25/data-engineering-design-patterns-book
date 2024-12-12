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
             'end_date': pendulum.parse("2024-09-22"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval='@daily', catchup=True) as dag):
    BASE_DIR = '/tmp/dedp/ch04/03-database/01-keyed-idempotency-airflow-files/input'
    next_file_path = BASE_DIR + '/{{ logical_date | ds_nodash }}.json'
    wait_for_the_input_file = FileSensor(
        task_id='wait_for_the_input_file',
        filepath=next_file_path,
        mode='reschedule',
    )

    @task
    def transform_json_to_csv(current_execution_date, base_dir):
        import json
        import csv

        csv_data_file = open(f'{base_dir}/{current_execution_date}.csv', 'w')
        csv_writer = csv.writer(csv_data_file)

        with open(f'{base_dir}/{current_execution_date}.json', "r") as json_file:
            json_lines = json_file.readlines()
            line_number = 0
            for json_line in json_lines:
                json_data = json.loads(json_line)
                if line_number == 0:
                    header = json_data.keys()
                    csv_writer.writerow(header)
                line_number += 1
                csv_writer.writerow(json_data.values())

        csv_data_file.close()

    (wait_for_the_input_file >>
     transform_json_to_csv(current_execution_date='{{ logical_date | ds_nodash }}', base_dir=BASE_DIR))
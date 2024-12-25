import logging
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG('devices_loader', max_active_runs=5,
         default_args={
             'depends_on_past': False,
             'start_date': pendulum.yesterday(tz='utc'),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         schedule_interval=None) as dag:
    @task
    def copy_dataset():
        import shutil
        from airflow.operators.python import get_current_context

        dataset_dir = '/tmp/dedp/ch02/06-event-driven/01-external-trigger-lambda-airflow/output/'
        os.makedirs(dataset_dir, exist_ok=True)
        context = get_current_context()
        dag_run = context["dag_run"]
        dagrun_conf = dag_run.conf
        logging.info(f'Configuration: {dagrun_conf}')
        shutil.copyfile(dagrun_conf['file_to_load'],
                        f"{dataset_dir}/devices_{dagrun_conf['trigger']['lambda_request_id']}.json")

    copy_dataset()


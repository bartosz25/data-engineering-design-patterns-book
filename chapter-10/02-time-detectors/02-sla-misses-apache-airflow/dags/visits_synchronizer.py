import datetime
import os
import time
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import task


def my_sla_miss_callback(dag_missed, task_list, blocking_task_list, slas, blocking_tis):
    print(f'Missed callback: {dag_missed} // {task_list} // {blocking_task_list} // {slas} // {blocking_tis}')


with DAG('visits_synchronizer', max_active_runs=1,
         default_args={
             'depends_on_past': False,
             'start_date': pendulum.parse("2024-07-01"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
        sla_miss_callback=my_sla_miss_callback,
         template_searchpath=[os.getcwd()],
         schedule_interval=datetime.timedelta(minutes=3), catchup=False) as dag:


    @task
    def processing_task_1():
        #time.sleep(20)
        print('task 1')


    @task(
        sla=datetime.timedelta(seconds=10)
    )
    def processing_task_2():
        #time.sleep(5)
        print('task 2')

    processing_task_1() >> processing_task_2()

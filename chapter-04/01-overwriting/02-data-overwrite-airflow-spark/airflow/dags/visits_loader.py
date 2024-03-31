import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.sensors.filesystem import FileSensor

from config import get_data_location_base_dir, get_namespace

with DAG('visits_loader', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             # the time interval corresponds to the data generator dates
             'start_date': pendulum.parse("2023-11-01"),
             'end_date': pendulum.parse("2023-11-03"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily") as dag:

    next_partition_template = get_data_location_base_dir() + '/date={{ data_interval_end | ds }}'
    current_partition_template = get_data_location_base_dir() + '/date={{ ds }}'

    # This is the Extract part
    next_partition_sensor = FileSensor(
        task_id='next_partition_sensor',
        filepath=next_partition_template,
        mode='reschedule',
        do_xcom_push=False
    )

    visits_loader_job_k8s_namespace = 'dedp-ch04'
    load_job_trigger = SparkKubernetesOperator(
        task_id='load_job_trigger',
        namespace=visits_loader_job_k8s_namespace,
        application_file='visits_loader.yaml',
        do_xcom_push=True
    )

    load_job_sensor = SparkKubernetesSensor(
        task_id='load_job_sensor',
        namespace=visits_loader_job_k8s_namespace,
        mode='reschedule',
        application_name="{{ task_instance.xcom_pull(task_ids='load_job_trigger')['metadata']['name'] }}",
        attach_log=True
    )

    next_partition_sensor >> load_job_trigger >> load_job_sensor

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.sensors.filesystem import FileSensor

with DAG('visits_converter', max_active_runs=1,
         default_args={
             'depends_on_past': False,
             'start_date': pendulum.datetime(2024, 2, 1),
             'end_date': pendulum.datetime(2024, 2, 5),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily", catchup=True) as dag:
    input_dir = '/tmp/dedp/ch06/03-fan-out/02-exclusive-choice-airflow-migration/input'
    file_sensor = FileSensor(
        task_id='input_dataset_waiter',
        mode='reschedule',
        poke_interval=10,
        filepath=input_dir + '/date={{ ds_nodash }}/dataset.json'
    )

    def get_output_format_route(**context):
        migration_date = pendulum.datetime(2024, 2, 3)
        execution_date = context['execution_date']
        if execution_date >= migration_date:
            return 'load_job_trigger_delta'
        else:
            return 'load_job_trigger_csv'

    format_router = BranchPythonOperator(
        task_id='format_router',
        python_callable=get_output_format_route,
        provide_context=True
    )

    visits_loader_job_k8s_namespace = 'dedp-ch06'
    for output_format in ['delta', 'csv']:
        load_job_trigger = SparkKubernetesOperator(
            task_id=f'load_job_trigger_{output_format}',
            namespace=visits_loader_job_k8s_namespace,
            application_file='visits_converter.yaml',
            do_xcom_push=True,
            params={'output_format': output_format}
        )

        load_job_sensor = SparkKubernetesSensor(
            task_id=f'load_job_sensor_{output_format}',
            namespace=visits_loader_job_k8s_namespace,
            mode='reschedule',
            application_name="{{ task_instance.xcom_pull(task_ids='" + load_job_trigger.task_id + "')['metadata']['name'] }}",
            attach_log=True
        )

        file_sensor >> format_router >> load_job_trigger >> load_job_sensor

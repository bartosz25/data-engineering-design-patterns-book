import json
import os
from datetime import timedelta
from typing import Dict, Any

import pendulum
from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import task, get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.sensors.filesystem import FileSensor

with DAG('devices_loader', max_active_runs=3,
         default_args={
             'depends_on_past': False,
             'start_date': pendulum.parse("2024-01-01"),
             'end_date': pendulum.parse("2024-01-05"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily") as dag:
    backfilling_config_file_name = 'backfilling_configuration_'
    dataset_base_dir = '/tmp/dedp/ch03/concurrent-late-data-integrator/dataset/devices'
    next_partition_template = dataset_base_dir + '/event_time={{ data_interval_end | ds }}'
    current_partition_template = dataset_base_dir + '/event_time={{ ds }}'

    namespace = 'dedp-ch03'
    processing_marker = SparkKubernetesOperator(
        task_id='mark_partition_as_processing',
        namespace=namespace,
        application_file='processing_in_progress_partition_marker.yaml',
        do_xcom_push=True,
        watch=True,
        depends_on_past=True
    )

    next_partition_sensor = FileSensor(
        task_id='next_partition_sensor',
        filepath=next_partition_template,
        mode='reschedule',
        do_xcom_push=False
    )


    @task
    def process_devices():
        print('...processing devices; it''s a dummy task as we focus on the pattern here')

    already_processed_marker = SparkKubernetesOperator(
        task_id='mark_partition_as_processed',
        namespace=namespace,
        application_file='processing_done_partition_marker.yaml',
        watch=True,
        depends_on_past=True # It might not be necessary if you updater can handle conflicts; we keep it easy here and simply avoid conflicts
    )

    ## Branch
    backfill_creation_job = SparkKubernetesOperator(
        task_id='backfill_creation_job',
        namespace=namespace,
        application_file='backfill_creation_job.yaml',
        do_xcom_push=True,
        depends_on_past=True, # pretty important to ensure 1 run at a time and avoid concurrency issue sin the resolution
        params={'config_file_name': backfilling_config_file_name},
        watch=True
    )


    @task
    def generate_backfilling_arguments():
        context = get_current_context()
        current_partition_date = context['ds']
        dag_run: DagRun = context['dag_run']
        dag_run_start_time: str = dag_run.start_date.isoformat()

        def _extract_event_time(partition: str) -> str:
            return partition.replace('event_time=', '')

        def _run_id_for_event_time(event_time: str) -> str:
            return f'backfill_{_extract_event_time(event_time)}_from_{current_partition_date}_{dag_run_start_time}'

        configuration = read_backfilling_configuration(current_partition_date)
        return list(map(lambda partition: {
            'execution_date': _extract_event_time(partition),
            'trigger_run_id': _run_id_for_event_time(partition)
        }, configuration['partitions']))


    backfill_arguments_generator_from_config_file = generate_backfilling_arguments()
    backfill_triggers = TriggerDagRunOperator.partial(
        task_id='backfill_past_partitions',
        trigger_dag_id='devices_loader',
        reset_dag_run=True
    ).expand_kwargs(backfill_arguments_generator_from_config_file)


    def read_backfilling_configuration(execution_time) -> Dict[str, Any]:
        configuration_file_name = f'/tmp/dedp/ch03/concurrent-late-data-integrator/dataset/backfilling_configuration_{execution_time}.json'
        print(f'Reading {configuration_file_name}')
        with open(configuration_file_name, 'r') as backfilling_file:
            return json.load(backfilling_file)


    # current partition branch
    processing_marker >> next_partition_sensor >> process_devices() >> already_processed_marker

    already_processed_marker >> backfill_creation_job >> backfill_arguments_generator_from_config_file

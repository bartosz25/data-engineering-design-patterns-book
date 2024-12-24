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
    late_data_config_file_name_prefix = 'late_data_configuration_'
    dataset_base_dir = '/tmp/dedp/ch03/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-concurrent-trigger/dataset/devices'
    next_partition_template = dataset_base_dir + '/event_time={{ data_interval_end | ds }}'
    current_partition_template = dataset_base_dir + '/event_time={{ ds }}'

    namespace = 'dedp-ch03-late-data-trigger'
    processing_marker = SparkKubernetesOperator(
        task_id='mark_partition_as_processing',
        namespace=namespace,
        application_file='job_mark_partition_being_processed.yaml',
        image='docker.io/library/dedp_ch03_late_data_integrator_trigger:latest',
        image_pull_policy='Never',
        deferrable=False,
        do_xcom_push=False,
        delete_on_termination=False,
        reattach_on_restart=False,
        random_name_suffix=False,
        depends_on_past=True
    )

    next_partition_sensor = FileSensor(
        task_id='next_partition_sensor',
        filepath=next_partition_template,
        mode='reschedule',
        do_xcom_push=False
    )

    process_data_and_mark_partition_as_processed = SparkKubernetesOperator(
        task_id='process_data_and_mark_partition_as_processed',
        namespace=namespace,
        application_file='job_processing_and_marker_partition_as_processed.yaml',
        image='docker.io/library/dedp_ch03_late_data_integrator_trigger:latest',
        image_pull_policy='Never',
        deferrable=False,
        do_xcom_push=False,
        delete_on_termination=False,
        reattach_on_restart=False,
        random_name_suffix=False,
    )

    ## Branch
    late_data_detection_job = SparkKubernetesOperator(
        task_id='late_data_detection_job',
        namespace=namespace,
        application_file='job_late_data_detection.yaml',
        depends_on_past=True, # pretty important to ensure 1 run at a time and avoid concurrency issue sin the resolution
        params={'config_file_name_prefix': late_data_config_file_name_prefix},
        image='docker.io/library/dedp_ch03_late_data_integrator_trigger:latest',
        image_pull_policy='Never',
        deferrable=False,
        do_xcom_push=False,
        delete_on_termination=False,
        reattach_on_restart=False,
        random_name_suffix=False,
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
        configuration_file_name = f'/tmp/dedp/ch03/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-concurrent-trigger//dataset/{late_data_config_file_name_prefix}{execution_time}.json'
        print(f'Reading {configuration_file_name}')
        with open(configuration_file_name, 'r') as late_data_config_file:
            return json.load(late_data_config_file)


    # current partition branch
    processing_marker >> next_partition_sensor >> process_data_and_mark_partition_as_processed
    # late data trigger
    process_data_and_mark_partition_as_processed >> late_data_detection_job >> backfill_arguments_generator_from_config_file

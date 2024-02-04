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

with DAG('devices_loader', max_active_runs=1,
         default_args={
             'depend_on_past': True,
             'start_date': pendulum.parse("2024-01-01"),
             'end_date': pendulum.parse("2024-01-02"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily") as dag:
    backfilling_config_file_name = 'backfilling_configuration.json'
    dataset_base_dir = '/tmp/dedp/ch03/late-data-integrator/dataset/devices'
    next_partition_template = dataset_base_dir + '/event_time={{ data_interval_end | ds }}'
    current_partition_template = dataset_base_dir + '/event_time={{ ds }}'

    # This is the Extract part
    next_partition_sensor = FileSensor(
        task_id='next_partition_sensor',
        filepath=next_partition_template,
        mode='reschedule',
        do_xcom_push=False
    )


    @task
    def process_devices():
        print('...processing devices; it''s a dummy task as we focus on the pattern here')

    namespace = 'dedp-ch03'
    backfilling_configuration_creation_job_trigger = SparkKubernetesOperator(
        task_id='backfilling_configuration_creation_job_trigger',
        namespace=namespace,
        application_file='backfill_configuration_preparator.yaml',
        do_xcom_push=True,
        params={'config_file_name': backfilling_config_file_name}
    )

    backfilling_configuration_creation_job_sensor = SparkKubernetesSensor(
        task_id='backfilling_configuration_creation_job_sensor',
        namespace=namespace,
        mode='reschedule',
        application_name="{{ task_instance.xcom_pull(task_ids='backfilling_configuration_creation_job_trigger')['metadata']['name'] }}",
        attach_log=True
    )


    @task
    def generate_backfilling_arguments():
        context = get_current_context()
        current_partition = context["execution_date"]
        dag_run: DagRun = context['dag_run']
        print(dag_run.start_date.isoformat())
        dag_run_start_time: str = dag_run.start_date.isoformat()

        def _run_id_for_event_time(event_time: str) -> str:
            return f'backfill_{event_time}_from_{current_partition}_{dag_run_start_time}'

        configuration = read_backfilling_configuration()
        return list(map(lambda partition: {
            'execution_date': partition['event_time'],
            'trigger_run_id': _run_id_for_event_time(partition['event_time'])
        }, configuration['partitions']))


    backfilling_data_provider_for_triggers = generate_backfilling_arguments()
    backfill_triggers = TriggerDagRunOperator.partial(
        task_id='backfill_past_partitions',
        trigger_dag_id='devices_loader',
        reset_dag_run=True
    ).expand_kwargs(backfilling_data_provider_for_triggers)


    def read_backfilling_configuration() -> Dict[str, Any]:
        with open(f'/tmp/dedp/ch03/late-data-integrator/dataset/{backfilling_config_file_name}', 'r') as backfilling_file:
            return json.load(backfilling_file)


    @task
    def update_last_processed_versions():
        backfilling_configuration = read_backfilling_configuration()
        last_processed_version = backfilling_configuration['lastProcessedVersion']
        with open('/tmp/_last_processed_version', 'w') as last_processed_version_file:
            last_processed_version_file.write(str(last_processed_version))


    last_processed_versions_updater = update_last_processed_versions()
    # current partition branch
    next_partition_sensor >> process_devices() >> last_processed_versions_updater
    # backfilling branch
    backfilling_configuration_creation_job_trigger >> backfilling_configuration_creation_job_sensor \
        >> last_processed_versions_updater
    # triggering backfilling in the end is important to avoid concurrent updates between the backfilled partitions and
    # the current partition
    last_processed_versions_updater >> backfilling_data_provider_for_triggers >> backfill_triggers

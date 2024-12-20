import json
import os
from datetime import timedelta
from typing import Dict, Any

import pendulum
from airflow import DAG
from airflow.models import DagRun
from airflow.operators.python import task, get_current_context, PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

with (DAG('devices_loader', max_active_runs=1,
         default_args={
             'depends_on_past': True,
             'start_date': pendulum.parse("2024-01-01"),
             'end_date': pendulum.parse("2024-01-03"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily") as dag):
    late_data_config_file = 'late_data_configuration.json'
    dataset_base_dir = '/tmp/dedp/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-sequential/dataset/devices'
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

    namespace = 'dedp-ch03-late-data-sequential'
    late_data_configuration_creation_job_trigger = SparkKubernetesOperator(
        task_id='late_data_configuration_creation_job_trigger',
        namespace=namespace,
        application_file='late_data_detector_job.yaml',
        do_xcom_push=False,
        params={'config_file_name': late_data_config_file}
    )

    @task
    def generate_backfilling_arguments():
        context = get_current_context()
        current_partition = context["execution_date"]
        dag_run: DagRun = context['dag_run']
        dag_run_start_time: str = dag_run.start_date.isoformat()

        def _run_id_for_event_time(event_time: str) -> str:
            return f'late_data_{event_time}_from_{current_partition}_{dag_run_start_time}'

        configuration = read_late_data_configuration()
        late_partitions_config = list(map(lambda partition: {
            'execution_date': partition['event_time'],
            'trigger_run_id': _run_id_for_event_time(partition['event_time'])
        }, configuration['partitions']))
        return late_partitions_config

    @task
    def process_late_partitions(config):
        print(f'...processing late partitions with the config {config}')

    def read_late_data_configuration() -> Dict[str, Any]:
        with open(f'/tmp/dedp/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-sequential/dataset/{late_data_config_file}', 'r') as backfilling_file:
            return json.load(backfilling_file)


    def update_last_processed_versions():
        late_data_configuration = read_late_data_configuration()
        last_processed_version = late_data_configuration['lastProcessedVersion']
        with open('/tmp/dedp/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-sequential/dataset/_last_processed_version', 'w') as last_processed_version_file:
            last_processed_version_file.write(str(last_processed_version))

    last_version_update_job = PythonOperator(
        task_id='last_version_update_job',
        trigger_rule=TriggerRule.ALL_DONE,
        python_callable=update_last_processed_versions
    )

    late_data_configuration_generator_job = generate_backfilling_arguments()
    # current partition branch
    next_partition_sensor >> process_devices() >> last_version_update_job
    # late data integration branch
    late_data_configuration_creation_job_trigger >> late_data_configuration_generator_job
    process_late_partitions.expand(config=late_data_configuration_generator_job) \
        >> last_version_update_job

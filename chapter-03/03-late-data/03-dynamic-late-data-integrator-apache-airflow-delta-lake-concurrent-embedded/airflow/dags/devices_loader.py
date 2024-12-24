import json
import os
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import task, get_current_context

from operators.spark_kubernetes_with_deferrable_driver_pod_operator import \
    SparkKubernetesOperatorWithDeferrableDriverPodOperator

with DAG('devices_loader', max_active_runs=2,
         default_args={
             'depends_on_past': False,
             'start_date': pendulum.parse("2024-01-01"),
             'end_date': pendulum.parse("2024-01-04"),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily") as dag:
    prefix_for_late_data_configuration_file_name = 'late_data_for'
    dataset_base_dir = '/tmp/dedp/ch03/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-concurrent-embedded/dataset/devices'
    next_partition_template = dataset_base_dir + '/event_time={{ data_interval_end | ds }}'
    current_partition_template = dataset_base_dir + '/event_time={{ ds }}'
    namespace = 'dedp-ch03-late-data-conc-emb'

    run_job_mark_partition_being_processed = SparkKubernetesOperatorWithDeferrableDriverPodOperator(
        task_id='mark_partition_as_being_processed',
        namespace=namespace,
        depends_on_past=True,
        application_file='job_mark_partition_being_processed.yaml',
        image='docker.io/library/dedp_ch03_late_data_integrator_embedded:latest',
        image_pull_policy='Never',
        deferrable=True,
        do_xcom_push=False,
        delete_on_termination=False,
        reattach_on_restart=False,
        random_name_suffix=False
    )
    process_data_and_mark_current_partition_as_processed = SparkKubernetesOperatorWithDeferrableDriverPodOperator(
        task_id='process_data_and_mark_current_partition_as_processed',
        namespace=namespace,
        application_file='job_processing_and_marker_partition_as_processed.yaml',
        image='docker.io/library/dedp_ch03_late_data_integrator_embedded:latest',
        image_pull_policy='Never',
        deferrable=True,
        do_xcom_push=False,
        delete_on_termination=False,
        reattach_on_restart=False,
        random_name_suffix=False
    )

    get_late_data_and_mark_as_in_progress = SparkKubernetesOperatorWithDeferrableDriverPodOperator(
        task_id='get_late_data_and_mark_as_in_progress',
        namespace=namespace,
        depends_on_past=True,
        application_file='job_late_data_detection.yaml',
        image='docker.io/library/dedp_ch03_late_data_integrator_embedded:latest',
        image_pull_policy='Never',
        params={'config_file_name_prefix': prefix_for_late_data_configuration_file_name},
        deferrable=True,
        do_xcom_push=False,
        delete_on_termination=False,
        reattach_on_restart=False,
        random_name_suffix=False
    )


    @task
    def generate_arguments_for_late_data_integration_jobs():
        def _extract_event_time(partition: str) -> str:
            return partition.replace('event_time=', '')
        context = get_current_context()
        current_partition_date = context['ds']
        late_data_config_full_path = f'/tmp/dedp/ch03/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-concurrent-embedded/dataset/{prefix_for_late_data_configuration_file_name}_{current_partition_date}.json'
        with open(late_data_config_full_path, 'r') as late_data_file:
            configuration = json.load(late_data_file)
            backfill_config = list(map(lambda partition: {
                'params': {'partition_time': _extract_event_time(partition)}
            }, configuration['partitions']))
            return backfill_config

    late_data_arguments_provider_for_triggers = generate_arguments_for_late_data_integration_jobs()
    late_data_integrator_and_marker_as_processed = SparkKubernetesOperatorWithDeferrableDriverPodOperator.partial(
        task_id='late_data_integrator_and_marker_as_processed',
        namespace=namespace,
        application_file='job_processing_and_marker_partition_as_processed.yaml',
        image='docker.io/library/dedp_ch03_late_data_integrator_embedded:latest',
        image_pull_policy='Never',
        deferrable=True,
        do_xcom_push=False,
        delete_on_termination=False,
        reattach_on_restart=False,
        random_name_suffix=False
    ).expand_kwargs(late_data_arguments_provider_for_triggers)

    # current partition branch
    run_job_mark_partition_being_processed >> process_data_and_mark_current_partition_as_processed
    # backfilling branch
    run_job_mark_partition_being_processed >> get_late_data_and_mark_as_in_progress
    get_late_data_and_mark_as_in_progress >> late_data_arguments_provider_for_triggers >> late_data_integrator_and_marker_as_processed

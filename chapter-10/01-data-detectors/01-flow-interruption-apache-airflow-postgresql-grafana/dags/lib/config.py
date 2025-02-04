from pathlib import Path


def get_data_location_base_dir() -> str:
    data_dir = '/tmp/dedp/ch10/01-data-detectors/01-flow-interruption-apache-airflow-postgresql-grafana/input'
    return data_dir


def get_data_output_base_dir() -> str:
    data_dir = '/tmp/dedp/ch10/01-data-detectors/01-flow-interruption-apache-airflow-postgresql-grafana/output'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    return data_dir


def get_current_partition_file_full_path(context, extension: str) -> str:
    partition_to_process = context['logical_date'].strftime('%Y-%m-%d')
    return f'{get_data_location_base_dir()}/date={partition_to_process}/dataset.{extension}'

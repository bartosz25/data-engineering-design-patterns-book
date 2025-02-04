import datetime
from pathlib import Path


def get_data_location_base_dir() -> str:
    data_dir = '/tmp/dedp/ch10/01-data-detectors/02-skew-detector-apache-airflow-postgresql/input'
    return data_dir


def get_data_output_base_dir() -> str:
    data_dir = '/tmp/dedp/ch10/01-data-detectors/02-skew-detector-apache-airflow-postgresql/output'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    return data_dir


def get_current_partition_file_full_path(partition_date: datetime.datetime, extension: str) -> str:
    partition_to_process = partition_date.strftime('%Y-%m-%d')
    return f'{get_data_location_base_dir()}/date={partition_to_process}/dataset.{extension}'

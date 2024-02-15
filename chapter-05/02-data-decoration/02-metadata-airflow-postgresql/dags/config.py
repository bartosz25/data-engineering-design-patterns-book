from pathlib import Path


def get_data_location_base_dir(from_docker: bool) -> str:
    if from_docker:
        return '/data_to_load'
    else:
        data_dir = '/tmp/dedp/ch05/02-decorator/02-metadata-airflow-postgresql/input'
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        return data_dir


def get_database() -> str:
    return 'dedp'


def get_database_final_schema() -> str:
    return 'dedp'


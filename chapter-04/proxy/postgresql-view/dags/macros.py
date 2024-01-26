from airflow.models import DagRun
from airflow.operators.python import get_current_context

from config import get_data_location_base_dir


def get_devices_table_name() -> str:
    context = get_current_context()
    dag_run: DagRun = context['dag_run']
    table_suffix = dag_run.start_date.strftime('%Y%m%d_%H%M%S')
    return f'dedp.devices_internal_{table_suffix}'


def get_input_csv_to_load() -> str:
    return f'{get_data_location_base_dir(True)}/dataset.csv'

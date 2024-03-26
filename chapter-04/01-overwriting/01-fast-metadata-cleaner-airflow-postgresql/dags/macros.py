import pendulum

from config import get_data_location_base_dir


def get_weekly_table_name(execution_date: pendulum.date) -> str:
    ds_nodash = f'week_{execution_date.week_of_year}_{execution_date.year}'
    return f'dedp.visits_{ds_nodash}'


def get_input_csv_to_load(ds: str) -> str:
    return f'{get_data_location_base_dir(True)}/date={ds}/dataset.csv'


import pendulum

from config import get_data_location_base_dir, get_database_final_schema


def get_staging_table_name(base_name: str, ds_nodash: str) -> str:
    return get_table_name(base_name, ds_nodash, get_database_staging_schema(), True)


def get_weekly_table_name(base_name: str, execution_date: pendulum.date) -> str:
    ds_nodash = f'week_{execution_date.week_of_year}_{execution_date.year}'
    return get_table_name(base_name, ds_nodash, get_database_final_schema(), False)


def get_input_csv_to_load(ds: str) -> str:
    return f'{get_data_location_base_dir(True)}/date={ds}/dataset.csv'


def get_table_name(base_name: str, ds_nodash: str, schema: str, is_staging: bool) -> str:
    temporal_table_base_name = base_name + '_' + ds_nodash
    if is_staging:
        return schema + '.' + temporal_table_base_name + '_staging'
    else:
        return schema + '.' + temporal_table_base_name

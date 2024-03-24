def get_table_name(ds_nodash: str) -> str:
    return 'devices_'+ds_nodash


def get_input_csv_to_load_for_host() -> str:
    return '/tmp/dedp/ch06/01-sequence/01-local-sequencer-airflow/input/dataset.csv'

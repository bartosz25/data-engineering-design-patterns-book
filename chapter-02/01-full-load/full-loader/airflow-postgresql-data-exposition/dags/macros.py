def get_table_name(ds_nodash: str) -> str:
    return 'devices_'+ds_nodash


def get_input_csv_to_load_for_host() -> str:
    return '/tmp/dedp/ch02/full-loader/data-exposition/input/dataset.csv'

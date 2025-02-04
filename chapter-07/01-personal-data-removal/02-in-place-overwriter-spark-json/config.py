def get_base_dir() -> str:
    return '/tmp/dedp/ch07/01-personal-data-removal/02-in-place-overwrite-delta-lake'


def get_input_table_dir() -> str:
    return f'{get_base_dir()}/input'


def get_staging_table_dir() -> str:
    return f'{get_base_dir()}/staging'


def get_output_table_dir() -> str:
    return f'{get_base_dir()}/output'

def get_base_dir() -> str:
    return '/tmp/dedp/ch08/01-partitioning/02-vertical-partitioner-apache-spark/'


def get_delta_users_table_dir() -> str:
    return f'{get_base_dir()}/table/users'


def get_delta_technical_table_dir() -> str:
    return f'{get_base_dir()}/table/technical'


def get_delta_visits_table_dir() -> str:
    return f'{get_base_dir()}/table/visits'

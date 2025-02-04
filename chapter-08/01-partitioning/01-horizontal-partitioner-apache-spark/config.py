def get_base_dir() -> str:
    return '/tmp/dedp/ch08/01-partitioning/01-horizontal-partitioner-apache-spark'


def get_delta_table_dir() -> str:
    return f'{get_base_dir()}/delta-users'


def get_json_table_dir() -> str:
    return f'{get_base_dir()}/json-users'

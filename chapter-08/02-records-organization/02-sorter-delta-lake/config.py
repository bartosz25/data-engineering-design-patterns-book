def get_base_dir() -> str:
    return '/tmp/dedp/ch08/02-records-organization/02-sorter-delta-lake'


def get_input_dir() -> str:
    return f'{get_base_dir()}/input'


def get_table_zordered_dir() -> str:
    return f'{get_base_dir()}/visits-table-zordered'


def get_table_flat_dir() -> str:
    return f'{get_base_dir()}/visits-table-flat'


def get_visit_id() -> str:
    return "140393799289728_0"

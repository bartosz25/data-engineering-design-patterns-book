def get_base_dir() -> str:
    return '/tmp/dedp/ch08/04-data-representation/02-denormalizer-star-schema'


def get_input_visits_dir() -> str:
    return f'{get_base_dir()}/input/*'


def get_output_visits_dir() -> str:
    return f'{get_base_dir()}/fact-visits'


def get_output_date_dir() -> str:
    return f'{get_base_dir()}/dim-date'


def get_output_month_dir() -> str:
    return f'{get_output_date_dir()}-month'


def get_output_quarter_dir() -> str:
    return f'{get_output_date_dir()}-quarter'


def get_output_page_dir() -> str:
    return f'{get_base_dir()}/dim-page'


def get_output_category_dir() -> str:
    return f'{get_output_page_dir()}-category'


def get_one_big_table_dir() -> str:
    return f'{get_base_dir()}/one-big-table'


def get_output_date_dir_star_schema() -> str:
    return f'{get_base_dir()}/dim-date-star'


def get_output_page_dir_star_schema() -> str:
    return f'{get_base_dir()}/dim-page-star'

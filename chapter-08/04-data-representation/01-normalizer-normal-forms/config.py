def get_base_dir() -> str:
    return '/tmp/dedp/ch08/04-data-representation/01-normalizer-normal-forms'


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


def get_normalized_visits_table_dir() -> str:
    return f'{get_base_dir()}/normalized-visits'

def get_normalized_users_table_dir() -> str:
    return f'{get_base_dir()}/normalized-users'


def get_normalized_visits_context_table_dir() -> str:
    return f'{get_base_dir()}/normalized-visits-context'


def get_normalized_visits_technical_browser_context_table_dir() -> str:
    return f'{get_base_dir()}/normalized-visits-browser-context'


def get_normalized_visits_technical_device_context_table_dir() -> str:
    return f'{get_base_dir()}/normalized-visits-device-context'


def get_normalized_ad_table_dir() -> str:
    return f'{get_base_dir()}/normalized-ad'


def get_normalized_pages_table_dir() -> str:
    return f'{get_base_dir()}/normalized-pages'


def get_normalized_pages_categories_table_dir() -> str:
    return f'{get_base_dir()}/normalized-pages-categories'

def get_base_dir() -> str:
    return '/tmp/dedp/ch11/02-data-value/02-sidecar-pattern-apache-spark-structured-streaming'


def get_devices_table_dir() -> str:
    return f'{get_base_dir()}/devices'

def get_devices_stats_table_dir() -> str:
    return f'{get_base_dir()}/devices_stats2'


def get_visits_enriched_raw_table_dir() -> str:
    return f'{get_base_dir()}/visits_enriched_raw'
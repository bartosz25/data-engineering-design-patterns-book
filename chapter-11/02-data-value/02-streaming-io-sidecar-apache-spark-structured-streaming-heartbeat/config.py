def get_base_dir() -> str:
    return '/tmp/dedp/ch11/02-data-value/02-sidecar-pattern-apache-spark-structured-streaming-heartbeat'



def get_visits_enriched_raw_table_dir() -> str:
    return f'{get_base_dir()}/visits_enriched_raw'
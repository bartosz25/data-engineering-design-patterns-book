def get_base_dir() -> str:
    return '/tmp/dedp/ch08/03-access-optimization/01-metadata-enhancer-apache-spark-apache-parquet'


def get_json_dir() -> str:
    return f'{get_base_dir()}/output-json'


def get_parquet_dir() -> str:
    return f'{get_base_dir()}/output-parquet'

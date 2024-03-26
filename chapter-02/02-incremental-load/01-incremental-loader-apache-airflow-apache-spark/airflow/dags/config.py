from pathlib import Path


def get_data_location_base_dir() -> str:
    data_dir = '/tmp/dedp/ch02/incremental_loader/input'
    return data_dir


def get_data_output_base_dir() -> str:
    data_dir = '/tmp/dedp/ch02/incremental_loader/output'
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    return data_dir


def get_namespace() -> str:
    return 'dedp-ch02'

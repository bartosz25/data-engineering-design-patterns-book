def get_table_name(ds_nodash: str) -> str:
    return 'devices_' + ds_nodash


def get_internal_storage_location_for_devices_file() -> str:
    return '/tmp/dedp/ch06/01-sequence/02-isolated-sequencer-external-trigger/input-internal/dataset.csv'

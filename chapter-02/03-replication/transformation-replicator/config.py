class DemoConfiguration:
    BASE_DIR: str = '/tmp/dedp/ch02/replication/transformation-replicator'
    INPUT_PATH: str = f'{BASE_DIR}/input'
    DEVICES_TABLE_PATH: str = f'{BASE_DIR}/devices-table'
    DEVICES_TABLE_NO_NAME_PATH: str = f'{BASE_DIR}/devices-table-no-name'
    DEVICES_TABLE_TRUNCATED_NAME_PATH: str = f'{BASE_DIR}/devices-table-truncated-name'
    OUTPUT_PATH: str = f'{BASE_DIR}/output'

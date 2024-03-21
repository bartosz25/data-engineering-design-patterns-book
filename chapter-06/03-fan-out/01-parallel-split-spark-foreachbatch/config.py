class DemoConfiguration:
    BASE_DIR: str = '/tmp/dedp/ch06/03-fan-out/01-parallel-split-spark'
    INPUT_PATH: str = f'{BASE_DIR}/input'
    DEVICES_TABLE: str = f'{BASE_DIR}/devices-table'
    DEVICES_TABLE_ENRICHED: str = f'{BASE_DIR}/devices-table-enriched'

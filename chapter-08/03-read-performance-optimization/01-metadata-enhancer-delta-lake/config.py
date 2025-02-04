class DemoConfiguration:
    BASE_DIR: str = '/tmp/dedp/ch08/03-access-optimization/01-metadata-enhancer-delta-lake'
    INPUT_PATH: str = f'{BASE_DIR}/input'
    DEVICES_TABLE_DELTA_LAKE: str = f'{BASE_DIR}/devices-table-delta-lake'
    DEVICES_TABLE_JSON: str = f'{BASE_DIR}/devices-table-json'

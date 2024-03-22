class DemoConfiguration:
    BASE_DIR: str = '/tmp/dedp/ch06/03-fan-out/02-exclusive-choice-spark/'
    INPUT_PATH: str = f'{BASE_DIR}/input'
    DEVICES_TABLE_LEGACY: str = f'{BASE_DIR}/output/devices-table'
    DEVICES_TABLE_SCHEMA_CHANGED: str = f'{BASE_DIR}/output/devices-table-schema-changed'

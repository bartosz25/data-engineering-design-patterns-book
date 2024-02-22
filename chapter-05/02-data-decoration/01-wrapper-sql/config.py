class DemoConfiguration:
    BASE_DIR: str = '/tmp/dedp/ch05/02-decorator/01-wrapper-sql'
    INPUT_PATH: str = f'{BASE_DIR}/input'
    TABLE_FLATTENED: str = f'{BASE_DIR}/visits-flattened-table'
    TABLE_DECORATED_STRUCT: str = f'{BASE_DIR}/visits-decorated-struct-table'
    TABLE_RAW_STRUCT: str = f'{BASE_DIR}/visits-raw-struct-table'

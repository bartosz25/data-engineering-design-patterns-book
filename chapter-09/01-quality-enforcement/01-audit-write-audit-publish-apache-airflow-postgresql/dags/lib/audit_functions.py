import os
from typing import Optional

import pandas
from airflow.utils.context import Context
from lib.config import get_current_partition_file_full_path


def validate_the_file_before_processing(context: Context, min_file_size_in_bytes: int, min_number_of_lines: int):
    file_to_test = get_current_partition_file_full_path(context, 'json')

    file_size = os.path.getsize(file_to_test)
    number_of_lines = 0
    invalid_json_line: Optional[str] = None
    invalid_json_line_number: Optional[int] = None
    with open(file_to_test, 'r') as f:
        for json_line_candidate in f.readlines():
            json_line_candidate_trimmed = json_line_candidate.strip()
            if not invalid_json_line and (
                    not json_line_candidate_trimmed.startswith('{') or not json_line_candidate_trimmed.endswith('}')):
                invalid_json_line = json_line_candidate_trimmed
                invalid_json_line_number = number_of_lines + 1
            number_of_lines += 1
    validation_errors = []
    if file_size < min_file_size_in_bytes:
        validation_errors.append(
            f'File is to small. Expected at least {min_file_size_in_bytes} bytes but got {file_size}')
    if number_of_lines < min_number_of_lines:
        validation_errors.append(
            f'File is too short. Expected at least {min_number_of_lines} lines but got {number_of_lines}')
    if invalid_json_line:
        validation_errors.append(
            f'File contains some invalid JSON lines. The first error found was {invalid_json_line}, line {invalid_json_line_number}')

    if validation_errors:
        raise Exception('Audit failed for the file: \n - ' + "\n - ".join(validation_errors))


def validate_flatten_visits(context: Context):
    required_columns = ['visit_id', 'event_time', 'user_id', 'page', 'ip', 'login', 'browser',
                        'browser_version', 'network_type', 'device_type', 'device_version']
    columns_with_nulls = []
    visits_to_validate = pandas.read_csv(get_current_partition_file_full_path(context, 'csv'), sep=';', header=0)
    for validated_column in required_columns:
        if visits_to_validate[validated_column].isnull().any():
            columns_with_nulls.append(validated_column)

    if columns_with_nulls:
        raise Exception('Found nulls in not nullable columns: ' + ', '.join(columns_with_nulls))

{% set weekly_table = get_weekly_table_name(execution_date) %}

COPY {{ weekly_table }}  FROM '{{ get_input_csv_to_load(ds) }}' CSV  DELIMITER ';' HEADER;
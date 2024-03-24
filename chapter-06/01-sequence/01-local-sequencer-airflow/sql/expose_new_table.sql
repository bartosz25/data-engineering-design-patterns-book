{% set table_name = get_table_name(ds_nodash) %}
CREATE OR REPLACE VIEW devices AS SELECT * FROM {{ table_name }}
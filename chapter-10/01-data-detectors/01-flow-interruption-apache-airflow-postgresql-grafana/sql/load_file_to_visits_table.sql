DELETE FROM dedp.visits_flattened WHERE execution_time = '{{ execution_date }}';

COPY dedp.visits_flattened FROM '/data_to_load/date={{ ds }}/dataset.csv' CSV  DELIMITER ';' HEADER;
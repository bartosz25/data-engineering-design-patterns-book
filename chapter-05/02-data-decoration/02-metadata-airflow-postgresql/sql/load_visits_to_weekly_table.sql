{% set weekly_table = get_weekly_table_name(execution_date) %}

CREATE TEMPORARY TABLE tmp_devices (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    keep_private BOOLEAN NOT NULL,
    page VARCHAR(20) NULL,
    context TEXT NOT NULL
);

COPY tmp_devices FROM '{{ get_input_csv_to_load(ds) }}' CSV  DELIMITER ';' HEADER;
DELETE FROM dedp.visits_context WHERE execution_date_time = '{{ execution_date }}';
INSERT INTO dedp.visits_context (execution_date_time, loading_time, code_version, loading_attempt)
VALUES ('{{ execution_date }}', '{{ dag_run.start_date }}', '{{ params.code_version }}', {{ task_instance.try_number }});

INSERT INTO {{ weekly_table }}
    (SELECT tmp_devices.*, '{{ execution_date }}' AS visits_context_execution_date_time FROM tmp_devices);

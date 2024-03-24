CREATE TEMPORARY TABLE visits_temp (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    keep_private BOOLEAN NOT NULL,
    page VARCHAR(20) NOT NULL,
    context TEXT NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);

COPY visits_temp FROM '/data_to_load/date={{ ds_nodash }}/dataset.csv' CSV  DELIMITER ';' HEADER;

DELETE FROM dedp.visits_trend WHERE current_execution_time_id = '{{ ds }}';

INSERT INTO dedp.visits_trend (current_execution_time_id, visits_trend)
    SELECT  '{{ ds }}',
        (SELECT COUNT(*) AS current_count FROM dedp.visits_raw WHERE execution_time_id = '{{ ds }}') -
        (SELECT COUNT(*) AS previous_count FROM dedp.visits_raw WHERE execution_time_id = '{{ prev_ds }}') AS
        visits_trend
;

CREATE TEMPORARY TABLE visits_temp (
    visit_id TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    keep_private BOOLEAN NOT NULL,
    page VARCHAR(20) NOT NULL,
    context TEXT NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);

COPY visits_temp FROM '/data_to_load/date={{ ds_nodash }}/dataset.csv' CSV  DELIMITER ';' HEADER;

DELETE FROM dedp.visits_raw WHERE execution_time_id = '{{ ds }}';

INSERT INTO dedp.visits_raw (visit_id, execution_time_id, user_id, page)
    SELECT visit_id, '{{ ds }}', user_id, page
    FROM visits_temp;
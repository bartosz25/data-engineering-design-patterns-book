-- temporary table; can be avoided if the data is already present in the database
CREATE TEMPORARY TABLE visits_{{ ds_nodash }} (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    keep_private BOOLEAN NOT NULL,
    page VARCHAR(20) NOT NULL,
    context TEXT NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);

COPY visits_{{ ds_nodash }} FROM '/data_to_load/date={{ ds_nodash }}/dataset.csv' CSV  DELIMITER ';' HEADER;

-- first, let's combine the incoming rows with the existing pending sessions
CREATE TEMPORARY TABLE sessions_to_classify AS
    SELECT
        COALESCE(p.session_id, n.session_id) AS session_id,
        COALESCE(p.user_id, n.user_id) AS user_id,
        '{{ ds }}' AS execution_time_id,
        LEAST(p.start_time, n.start_time) AS start_time,
        GREATEST(p.last_visit_time, n.start_time) AS last_visit_time,
        ARRAY_CAT(p.pages, n.pages) AS pages,
        CASE
            WHEN n.user_id IS NULL THEN p.expiration_batch_id -- no new data; keep the previous date
            ELSE '{{ macros.ds_add(ds, 2) }}' -- previous data and new data // or only new data
        END AS expiration_batch_id
    FROM (
        SELECT
                CONCAT_WS('-', visit_id, user_id) AS session_id,
                user_id,
                ARRAY_AGG(page) OVER visits_window AS pages,
                FIRST_VALUE(event_time) OVER visits_window AS start_time,
                LAST_VALUE(event_time) OVER visits_window AS last_visit_time
            FROM
                visits_{{ ds_nodash }}
            WINDOW
                visits_window AS (PARTITION BY visit_id, user_id ORDER BY event_time)
    ) AS n
    FULL OUTER JOIN (
        SELECT session_id, user_id, pages, start_time, last_visit_time, expiration_batch_id
        FROM dedp.pending_sessions
        WHERE execution_time_id = '{{ prev_ds }}'
    ) AS p ON n.session_id = p.session_id
;

INSERT INTO dedp.pending_sessions (session_id, user_id, start_time, last_visit_time, pages,
expiration_batch_id, execution_time_id)
    SELECT session_id, user_id, start_time,
        last_visit_time, pages, expiration_batch_id, execution_time_id
    FROM sessions_to_classify WHERE expiration_batch_id != '{{ ds }}';

INSERT INTO dedp.sessions (session_id, execution_time_id, user_id, start_time, end_time, pages)
    SELECT session_id, execution_time_id, user_id, start_time,
        last_visit_time AS end_time, pages
    FROM sessions_to_classify WHERE expiration_batch_id = '{{ ds }}';
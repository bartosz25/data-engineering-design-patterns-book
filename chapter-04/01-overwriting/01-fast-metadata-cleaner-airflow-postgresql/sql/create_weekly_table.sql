{% set table_to_create = get_weekly_table_name(execution_date) %}

DROP TABLE IF EXISTS {{ table_to_create }} CASCADE;
CREATE TABLE {{ table_to_create }} (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    keep_private BOOLEAN NOT NULL,
    page VARCHAR(20) NULL,
    context TEXT NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);

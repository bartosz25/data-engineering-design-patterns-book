CREATE SCHEMA dedp;

CREATE TABLE dedp.visits_raw (
    visit_id TEXT NOT NULL,
    execution_time_id CHAR(10) NOT NULL,
    user_id  TEXT NOT NULL,
    page TEXT NOT NULL,
    PRIMARY KEY(visit_id, execution_time_id)
);

CREATE TABLE dedp.visits_trend (
    current_execution_time_id CHAR(10) NOT NULL,
    visits_trend INT NOT NULL,
    PRIMARY KEY(current_execution_time_id)
);
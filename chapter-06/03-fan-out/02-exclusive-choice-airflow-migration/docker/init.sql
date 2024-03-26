CREATE SCHEMA dedp;

CREATE TABLE dedp.visits_raw (
    execution_time_id CHAR(10) NOT NULL,
    execution_time_hour_id CHAR(2) NOT NULL,
    user_id  TEXT NOT NULL,
    page TEXT NOT NULL,
    PRIMARY KEY(user_id, page, execution_time_id, execution_time_hour_id)
);

CREATE TABLE dedp.visits_cube (
    current_execution_time_id CHAR(10) NOT NULL,
    page TEXT,
    user_id TEXT,
    is_approximate BOOL,
    visits_number INT NOT NULL
);
CREATE SCHEMA dedp;

CREATE TABLE dedp.visits_raw (
    execution_time_id CHAR(13) NOT NULL,
    user_id  TEXT NOT NULL,
    page TEXT NOT NULL,
    PRIMARY KEY(user_id, page, execution_time_id)
);

CREATE TABLE dedp.visits_cube (
    current_execution_time_id CHAR(10) NOT NULL,
    page TEXT,
    user_id TEXT,
    visits_number INT NOT NULL
);
CREATE SCHEMA dedp;

CREATE TABLE dedp.visits_input (
    id SERIAL PRIMARY KEY,
    visit_id CHAR(36) NULL,
    event_time TIMESTAMP NULL,
    user_id  TEXT NULL,
    page VARCHAR(20) NULL,
    context JSONB NULL
);

CREATE TABLE dedp.visits_output (
    id SERIAL PRIMARY KEY,
    visit_id CHAR(36) NULL,
    event_time TIMESTAMP NULL,
    user_id  TEXT NULL,
    page VARCHAR(20) NULL,
    context JSONB NULL
);
CREATE TABLE dedp.visits_state (
    execution_time TIMESTAMP NOT NULL PRIMARY KEY,
    first_row_id INT NOT NULL,
    last_row_id INT NOT NULL
);

CREATE TABLE dedp.visits_monitoring_state (
    execution_time TIMESTAMP NOT NULL PRIMARY KEY,
    first_row_id INT NOT NULL,
    last_row_id INT NOT NULL
);

CREATE TABLE dedp.visits_monitoring (
    execution_time TIMESTAMP NOT NULL,
    processing_time TIMESTAMP NOT NULL DEFAULT NOW(),
    all_rows INT NOT NULL,
    invalid_event_time INT NOT NULL DEFAULT 0,
    invalid_user_id INT NOT NULL DEFAULT 0,
    invalid_page INT NOT NULL DEFAULT 0,
    invalid_context INT NOT NULL DEFAULT 0,
    PRIMARY KEY(execution_time, processing_time)
);

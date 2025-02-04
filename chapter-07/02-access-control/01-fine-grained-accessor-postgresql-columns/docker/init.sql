CREATE SCHEMA dedp;

CREATE TABLE dedp.users (
    id TEXT NOT NULL,
    login VARCHAR(45) NOT NULL,
    email VARCHAR(45) NULL,
    registered_datetime TIMESTAMP NOT NULL,
    first_connection_datetime TIMESTAMP NULL,
    last_connection_datetime TIMESTAMP NULL,
    PRIMARY KEY(id)
);

CREATE TABLE dedp.visits (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    page VARCHAR(20) NULL,
    context JSONB NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);
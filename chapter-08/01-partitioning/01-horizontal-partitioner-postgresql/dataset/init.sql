CREATE SCHEMA dedp;

CREATE TABLE dedp.visits_all_20231124 (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    page VARCHAR(20) NULL,
    context JSONB NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);

CREATE TABLE dedp.visits_all_20231125 (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    page VARCHAR(20) NULL,
    context JSONB NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);

CREATE TABLE dedp.visits_all_20231126 (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    page VARCHAR(20) NULL,
    context JSONB NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);

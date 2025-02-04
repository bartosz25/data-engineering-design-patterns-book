CREATE SCHEMA dedp;

CREATE TABLE dedp.visits_all (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    page VARCHAR(20) NULL,
    context JSONB NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);

CREATE SCHEMA dedp;

CREATE TABLE dedp.visits (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    page VARCHAR(20) NULL,
    insertion_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(visit_id, event_time)
);

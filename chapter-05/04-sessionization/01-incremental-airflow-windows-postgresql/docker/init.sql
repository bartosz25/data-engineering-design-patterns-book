CREATE SCHEMA dedp;

CREATE TABLE dedp.pending_sessions (
    session_id TEXT NOT NULL,
    execution_time_id CHAR(10) NOT NULL,
    user_id  TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    last_visit_time TIMESTAMP NOT NULL,
    pages TEXT ARRAY NOT NULL,
    expiration_batch_id CHAR(10) NOT NULL,
    PRIMARY KEY(session_id, execution_time_id)
);
CREATE INDEX ON dedp.pending_sessions (execution_time_id);

CREATE TABLE dedp.sessions (
    session_id TEXT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    execution_time_id CHAR(10) NOT NULL,
    end_time TIMESTAMP NOT NULL,
    pages TEXT ARRAY NOT NULL,
    user_id  TEXT NOT NULL,
    PRIMARY KEY(session_id)
);
CREATE INDEX ON dedp.sessions (execution_time_id);
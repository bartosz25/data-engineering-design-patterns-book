CREATE SCHEMA dedp;

CREATE TABLE dedp.visits (
    visit_id VARCHAR(10) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id VARCHAR(25) NOT NULL,
    page VARCHAR(25) NOT NULL,
    ip VARCHAR(25) NOT NULL,
    login VARCHAR(25) NOT NULL,
    is_connected BOOL NOT NULL DEFAULT false,
    from_page VARCHAR(25) NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);


CREATE TABLE dedp.connected_users_visits (
    visit_id VARCHAR(10) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id VARCHAR(25) NOT NULL,
    page VARCHAR(25) NOT NULL,
    ip VARCHAR(25) NOT NULL,
    login VARCHAR(25) NOT NULL,
    from_page VARCHAR(25) NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);

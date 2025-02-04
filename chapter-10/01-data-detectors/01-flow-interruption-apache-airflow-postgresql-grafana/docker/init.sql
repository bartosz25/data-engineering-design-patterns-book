CREATE SCHEMA dedp;

CREATE TABLE dedp.visits_flattened (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    keep_private BOOLEAN NOT NULL,
    page VARCHAR(20) NOT NULL,
    referral VARCHAR(50) NULL,
    ad_id VARCHAR(50) NULL,
    ip VARCHAR(40) NOT NULL,
    login VARCHAR(40) NOT NULL,
    connected_since TIMESTAMP NULL,
    browser VARCHAR(40) NOT NULL,
    browser_version VARCHAR(40) NOT NULL,
    network_type VARCHAR(15) NOT NULL,
    device_type VARCHAR(15) NOT NULL,
    device_version VARCHAR(55) NOT NULL,
    execution_time TIMESTAMP NOT NULL,
    PRIMARY KEY(visit_id, user_id, event_time, page, execution_time)
);

CREATE SCHEMA dedp;

CREATE TABLE dedp.devices (
    type VARCHAR(10) NOT NULL,
    full_name TEXT NOT NULL,
    version VARCHAR(25) NOT NULL,
    PRIMARY KEY(type, version)
);
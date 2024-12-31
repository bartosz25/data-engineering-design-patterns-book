CREATE SCHEMA dedp;

CREATE TABLE dedp.versions (
    execution_time TEXT NOT NULL,
    table_name TEXT NOT NULL,
    version INT NOT NULL,
    PRIMARY KEY (execution_time)
);

CREATE TABLE dedp.devices (
    id TEXT NOT NULL,
    brand_name TEXT NOT NULL,
    full_name TEXT NOT NULL,
    processor_brand TEXT NOT NULL,
    PRIMARY KEY(id)
);

CREATE TABLE dedp.devices_history (
    id TEXT NOT NULL,
    execution_time TIMESTAMP NOT NULL,
    brand_name TEXT NOT NULL,
    full_name TEXT NOT NULL,
    processor_brand TEXT NOT NULL,
    PRIMARY KEY(id, execution_time)
);


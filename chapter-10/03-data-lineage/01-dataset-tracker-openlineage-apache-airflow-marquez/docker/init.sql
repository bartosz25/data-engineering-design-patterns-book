CREATE SCHEMA dedp;


CREATE TABLE dedp.devices_raw (
    type VARCHAR(15) NOT NULL,
    full_name VARCHAR(50) NOT NULL,
    version VARCHAR(40) NOT NULL,
    load_id CHAR(8) NOT NULL,
    PRIMARY KEY(type, full_name, version, load_id)
);

CREATE TABLE dedp.devices_aggregates (
    type VARCHAR(15) NOT NULL,
    full_name VARCHAR(50) NOT NULL,
    version VARCHAR(40) NOT NULL,
    all_occurrences INT NOT NULL,
    PRIMARY KEY(type, full_name, version)
);


CREATE TABLE dedp.devices_aggregates_bi (
    type VARCHAR(15) NOT NULL,
    full_name VARCHAR(50) NOT NULL,
    version VARCHAR(40) NOT NULL,
    all_occurrences INT NOT NULL,
    PRIMARY KEY(type, full_name, version)
);


CREATE TABLE devices_raw (
    type VARCHAR(15) NOT NULL,
    full_name VARCHAR(50) NOT NULL,
    version VARCHAR(40) NOT NULL,
    load_id CHAR(8) NOT NULL,
    PRIMARY KEY(type, full_name, version, load_id)
);

CREATE TABLE devices_aggregates (
    type VARCHAR(15) NOT NULL,
    full_name VARCHAR(50) NOT NULL,
    version VARCHAR(40) NOT NULL,
    all_occurrences INT NOT NULL,
    PRIMARY KEY(type, full_name, version)
);


CREATE TABLE devices_aggregates_bi (
    type VARCHAR(15) NOT NULL,
    full_name VARCHAR(50) NOT NULL,
    version VARCHAR(40) NOT NULL,
    all_occurrences INT NOT NULL,
    PRIMARY KEY(type, full_name, version)
);
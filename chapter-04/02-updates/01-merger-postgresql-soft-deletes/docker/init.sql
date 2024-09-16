CREATE SCHEMA dedp;

CREATE TABLE dedp.devices_input (
    type VARCHAR(10) NOT NULL,
    full_name TEXT NOT NULL,
    version VARCHAR(25) NOT NULL,
    is_deleted BOOL NOT NULL DEFAULT false,
    PRIMARY KEY(type, version)
);
CREATE TABLE dedp.devices_output (
    type VARCHAR(10) NOT NULL,
    full_name TEXT NOT NULL,
    version VARCHAR(25) NOT NULL,
    PRIMARY KEY(type, version)
);

INSERT INTO dedp.devices_input (type, full_name, version) VALUES
('galaxy', 'Galaxy Camera', 'Android 11'),
('iphone', 'APPLE iPhone 8 Plus (Silver, 256 GB)', 'iOS 13'),
('htc', 'Evo 3d', 'Android 12L');
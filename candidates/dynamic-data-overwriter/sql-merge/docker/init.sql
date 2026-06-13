CREATE SCHEMA dedp;

CREATE TABLE dedp.devices_output (
    type VARCHAR(10) NOT NULL,
    full_name TEXT NOT NULL,
    version VARCHAR(25) NOT NULL,
    PRIMARY KEY(type, version)
);

INSERT INTO dedp.devices_output (type, full_name, version) VALUES
('galaxy', 'Galaxy Camera', 'Android 11'),
('galaxy', 'Galaxy Camera', 'Android 12'),
('iphone', 'APPLE iPhone 8 Plus (Silver, 256 GB)', 'iOS 13'),
('iphone', 'APPLE iPhone 9 Plus (Silver, 256 GB)', 'iOS 14'),
('htc', 'Evo 3d', 'Android 12L');
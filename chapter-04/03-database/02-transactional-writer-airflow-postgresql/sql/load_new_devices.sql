-- I'm splitting the load into 2 steps on purpose
-- The goal is to demonstrate the transactions
CREATE TEMPORARY TABLE changed_devices_file1 (LIKE dedp.devices);
COPY changed_devices_file1 FROM '/data_to_load/dataset_1.csv' CSV  DELIMITER ';' HEADER;
MERGE INTO dedp.devices AS d
USING changed_devices_file1 AS c_d
ON c_d.type = d.type AND c_d.version = d.version
WHEN MATCHED THEN
    UPDATE SET
     full_name = c_d.full_name
WHEN NOT MATCHED THEN
    INSERT (type, full_name, version)
    VALUES (c_d.type, c_d.full_name, c_d.version);


CREATE TEMPORARY TABLE changed_devices_file2 (LIKE dedp.devices);
COPY changed_devices_file2 FROM '/data_to_load/dataset_too_long_type.csv' CSV  DELIMITER ';' HEADER;
MERGE INTO dedp.devices AS d
USING changed_devices_file2 AS c_d
ON c_d.type = d.type AND c_d.version = d.version
WHEN MATCHED THEN
    UPDATE SET
     full_name = c_d.full_name
WHEN NOT MATCHED THEN
    INSERT (type, full_name, version)
    VALUES (c_d.type, c_d.full_name, c_d.version);


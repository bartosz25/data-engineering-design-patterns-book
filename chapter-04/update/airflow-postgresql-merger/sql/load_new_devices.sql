CREATE TEMPORARY TABLE changed_devices (LIKE dedp.devices);

COPY changed_devices FROM '/data_to_load/dataset.csv' CSV  DELIMITER ';' HEADER;


MERGE INTO dedp.devices AS d
USING changed_devices AS c_d
ON c_d.type = d.type AND c_d.version = d.version
WHEN MATCHED THEN
    UPDATE SET
     full_name = c_d.full_name
WHEN NOT MATCHED THEN
    INSERT (type, full_name, version)
    VALUES (c_d.type, c_d.full_name, c_d.version)
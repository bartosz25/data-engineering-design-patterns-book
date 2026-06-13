# Dynamic Data Overwriter - PostgreSQL and MERGE
1. Start the PostgreSQL database:
```shell
cd docker
docker-compose down --volumes; docker-compose up
```

2. Let's start by loading the input data to our output table:
```shell
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
```
Next, run this query to check the output table:
```
dedp=# SELECT * FROM dedp.devices_output;
  type  |              full_name               |   version   
--------+--------------------------------------+-------------
 galaxy | Galaxy Camera                        | Android 11
 galaxy | Galaxy Camera                        | Android 12
 iphone | APPLE iPhone 8 Plus (Silver, 256 GB) | iOS 13
 iphone | APPLE iPhone 9 Plus (Silver, 256 GB) | iOS 14
 htc    | Evo 3d                               | Android 12L
(5 rows)
```

3. In our case, we suppose the partitioning is done by the _type_ column. Consequently,
the `MERGE` statement from the next code snippet will replace all _galaxy_ devices by the ones
present in the loaded table:
```
dedp=# CREATE TEMPORARY TABLE devices_output_temp (
    type VARCHAR(10) NOT NULL,
    full_name TEXT NOT NULL,
    version VARCHAR(25) NOT NULL,
    PRIMARY KEY(type, version)
);

CREATE TABLE
dedp=# INSERT INTO devices_output_temp (type, full_name, version) VALUES
('galaxy', 'Galaxy Camera', 'Android 11.0'),
('galaxy', 'Galaxy Camera', 'Android 12.0'),
('lenovo', 'Lenovo 3d', 'Android 12.0.0');
INSERT 0 3

dedp=# SELECT * FROM devices_output_temp;
  type  |   full_name   |    version     
--------+---------------+----------------
 galaxy | Galaxy Camera | Android 11.0
 galaxy | Galaxy Camera | Android 12.0
 lenovo | Lenovo 3d     | Android 12.0.0
(3 rows)

dedp=# MERGE INTO dedp.devices_output AS target
USING devices_output_temp AS source
    ON target.type = source.type AND target.version = source.version
WHEN MATCHED THEN
    UPDATE SET full_name = source.full_name
WHEN NOT MATCHED BY TARGET THEN
    INSERT (type, full_name, version)
    VALUES (source.type, source.full_name, source.version)
WHEN NOT MATCHED BY SOURCE AND target.type IN (SELECT DISTINCT type FROM devices_output_temp) THEN
    DELETE;
MERGE 5

dedp=# SELECT * FROM dedp.devices_output;
  type  |              full_name               |    version     
--------+--------------------------------------+----------------
 iphone | APPLE iPhone 8 Plus (Silver, 256 GB) | iOS 13
 iphone | APPLE iPhone 9 Plus (Silver, 256 GB) | iOS 14
 htc    | Evo 3d                               | Android 12L
 galaxy | Galaxy Camera                        | Android 12.0
 galaxy | Galaxy Camera                        | Android 11.0
 lenovo | Lenovo 3d                            | Android 12.0.0
(6 rows)
```
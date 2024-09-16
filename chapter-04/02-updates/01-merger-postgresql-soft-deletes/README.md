# Merger - PostgreSQL & soft-deletes
1. Start the PostgreSQL database:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Let's start by loading the input data to our output table:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

dedp=# SELECT * FROM dedp.devices_output;
 type | full_name | version | is_deleted 
------+-----------+---------+------------
(0 rows)


dedp=#  MERGE INTO dedp.devices_output AS target
USING dedp.devices_input AS input
ON target.type = input.type AND target.version = input.version
WHEN MATCHED AND input.is_deleted = true THEN 
    DELETE
WHEN MATCHED AND input.is_deleted = false THEN 
    UPDATE SET full_name = input.full_name 
WHEN NOT MATCHED THEN
    INSERT (full_name, version, type) VALUES (input.full_name, input.version, input.type);
    
MERGE 3
dedp=# SELECT * FROM dedp.devices_output ORDER BY type, full_name;
  type  |              full_name               |   version   
--------+--------------------------------------+-------------
 galaxy | Galaxy Camera                        | Android 11
 htc    | Evo 3d                               | Android 12L
 iphone | APPLE iPhone 8 Plus (Silver, 256 GB) | iOS 13
(3 rows)

```

After the operation we should have 3 new rows in the output table. Let's see now the mixed scenario from the book.

4. The new dataset will insert a new row, update an existing one, and delete an existing one with the soft-deletes.

```
dedp=# TRUNCATE TABLE dedp.devices_input;
TRUNCATE TABLE

dedp=# INSERT INTO dedp.devices_input (type, full_name, version, is_deleted) VALUES
('galaxy', 'Galaxy Camera', 'Android 11', true),
('iphone', 'APPLE iPhone 28 Plus (Silver, 1256 GB)', 'iOS 130', false),
('htc', 'Evo 3d - mobile phone', 'Android 12L', false);
INSERT 0 3

dedp=# MERGE INTO dedp.devices_output AS target
USING dedp.devices_input AS input
ON target.type = input.type AND target.version = input.version
WHEN MATCHED AND input.is_deleted = true THEN 
    DELETE
WHEN MATCHED AND input.is_deleted = false THEN 
    UPDATE SET full_name = input.full_name 
WHEN NOT MATCHED THEN
    INSERT (full_name, version, type) VALUES (input.full_name, input.version, input.type);
MERGE 3

dedp=# SELECT * FROM dedp.devices_output ORDER BY type, full_name;
  type  |               full_name                |   version   
--------+----------------------------------------+-------------
 htc    | Evo 3d - mobile phone                  | Android 12L
 iphone | APPLE iPhone 28 Plus (Silver, 1256 GB) | iOS 130
 iphone | APPLE iPhone 8 Plus (Silver, 256 GB)   | iOS 13
(3 rows)
```

# Partitioning - vertical partitioner with PostgreSQL

1. Generate the dataset:
```
cd dataset
docker-compose down --volumes; docker-compose up
```

2. There are many ways to create a vertically partitioned table. The first is a simple sequence of the `SELECT` statements:

```
$ docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

CREATE TABLE dedp.users (
    visit_id CHAR(36) NOT NULL,
    ip  TEXT NOT NULL,
    login TEXT NOT NULL,
    user_id TEXT NOT NULL,
    PRIMARY KEY (visit_id));


INSERT INTO dedp.users (visit_id, ip, login, user_id) 
    (SELECT DISTINCT
    visit_id, context->'user'->>'ip', context->'user'->>'login', user_id
    FROM dedp.visits_all);

CREATE TABLE dedp.technical (
    visit_id CHAR(36) NOT NULL,
    browser  TEXT NOT NULL,
    browser_version  TEXT NOT NULL,
    network_type  TEXT NOT NULL,
    device_type  TEXT NOT NULL,
    device_version  TEXT NOT NULL,
    PRIMARY KEY (visit_id));

INSERT INTO dedp.technical (visit_id, browser, browser_version, network_type, device_type, device_version) 
    (SELECT DISTINCT
    visit_id, context->'technical'->>'browser', context->'technical'->>'browser_version', 
    context->'technical'->>'network_type', context->'technical'->>'device_type', context->'technical'->>'device_version'
    FROM dedp.visits_all);

CREATE TABLE dedp.visits (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    page VARCHAR(20) NULL,
    PRIMARY KEY(visit_id, event_time));
    
INSERT INTO dedp.visits (visit_id, event_time, page) 
    (SELECT visit_id, event_time, page FROM dedp.visits_all);
```

Let's verify the partitioned rows:

```
dedp=# SELECT * FROM dedp.users ORDER BY visit_id LIMIT 3;
               visit_id               |       ip        |    login     |                       user_id                        
--------------------------------------+-----------------+--------------+------------------------------------------------------
 139783366417280_0                    | 87.255.123.203  | khanmark     | 139783366417280_7adf4629-6043-4f4f-aac3-91e39244277b
 139783366417280_1                    | 130.157.254.160 | austin19     | 139783366417280_19ee9aab-c605-40a6-bb0e-b37159130010
 139783366417280_10                   | 105.154.26.85   | stewartdavid | 139783366417280_30da856d-3b3b-4290-a7b5-6d14819cb020
(3 rows)

dedp=# SELECT * FROM dedp.technical ORDER BY visit_id LIMIT 3;
               visit_id               | browser | browser_version | network_type | device_type | device_version 
--------------------------------------+---------+-----------------+--------------+-------------+----------------
 139783366417280_0                    | Chrome  | 23.1            | LAN          | iPad        | 3.0
 139783366417280_1                    | Safari  | 20.0            | 4G           | PC          | 1.0
 139783366417280_10                   | Safari  | 20.0            | Wi-Fi        | iPhone      | 4.0
(3 rows)

dedp=# SELECT * FROM dedp.visits ORDER BY visit_id LIMIT 3;
               visit_id               |     event_time      |  page  
--------------------------------------+---------------------+--------
 139783366417280_0                    | 2023-11-24 00:00:00 | index
 139783366417280_0                    | 2023-11-24 00:02:00 | page_8
 139783366417280_1                    | 2023-11-24 00:00:00 | about
(3 rows)
```

I omitted few columns and attributes for sake of simplicity.

3. An alternative way to use the `CREATE` + `INSERT ... SELECT` statements is to rely on the `CREATE TABLE AS SELECT...`.
   However, this approach is good if the table is written only once. Otherwise, you'll end up the aforementioned solution. 

```
CREATE TABLE dedp.users_select AS (SELECT DISTINCT
    visit_id, context->'user'->>'ip' AS ip, context->'user'->>'login' AS login, user_id
    FROM dedp.visits_all);


CREATE TABLE dedp.technical_select AS (SELECT DISTINCT
    visit_id, context->'technical'->>'browser' AS browser, context->'technical'->>'browser_version' AS browser_version, 
    context->'technical'->>'network_type' AS network_type, context->'technical'->>'device_type' AS device_type, 
    context->'technical'->>'device_version' AS device_version
    FROM dedp.visits_all);

CREATE TABLE dedp.visits_select AS (SELECT visit_id, event_time, page FROM dedp.visits_all);
```


You should get the same results as before:

```
dedp=# SELECT * FROM dedp.users_select ORDER BY visit_id LIMIT 3;
               visit_id               |       ip        |    login     |                       user_id                        
--------------------------------------+-----------------+--------------+------------------------------------------------------
 139783366417280_0                    | 87.255.123.203  | khanmark     | 139783366417280_7adf4629-6043-4f4f-aac3-91e39244277b
 139783366417280_1                    | 130.157.254.160 | austin19     | 139783366417280_19ee9aab-c605-40a6-bb0e-b37159130010
 139783366417280_10                   | 105.154.26.85   | stewartdavid | 139783366417280_30da856d-3b3b-4290-a7b5-6d14819cb020
(3 rows)

dedp=# SELECT * FROM dedp.technical_select ORDER BY visit_id LIMIT 3;
               visit_id               | browser | browser_version | network_type | device_type | device_version 
--------------------------------------+---------+-----------------+--------------+-------------+----------------
 139783366417280_0                    | Chrome  | 23.1            | LAN          | iPad        | 3.0
 139783366417280_1                    | Safari  | 20.0            | 4G           | PC          | 1.0
 139783366417280_10                   | Safari  | 20.0            | Wi-Fi        | iPhone      | 4.0
(3 rows)

dedp=# SELECT * FROM dedp.visits_select ORDER BY visit_id LIMIT 3;
               visit_id               |     event_time      |  page  
--------------------------------------+---------------------+--------
 139783366417280_0                    | 2023-11-24 00:00:00 | index
 139783366417280_0                    | 2023-11-24 00:02:00 | page_8
 139783366417280_1                    | 2023-11-24 00:00:00 | about
(3 rows)
```
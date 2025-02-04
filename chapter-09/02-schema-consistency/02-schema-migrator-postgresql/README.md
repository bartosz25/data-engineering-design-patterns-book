# Schema migrator - PostgreSQL
1. Start the PostgreSQL database:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Let's start by loading the input data to our output table:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

dedp=# SELECT * FROM dedp.visits;
 visit_id | event_time | user_id | page | ip | login | is_connected | from_page 
----------+------------+---------+------+----+-------+--------------+-----------
(0 rows)


dedp=# INSERT INTO dedp.visits (visit_id, event_time, user_id, page, ip, login, is_connected, from_page) VALUES 
('v1', NOW(), 'user 1', 'home.html', '1.1.1', 'user 1 login', true, 'google.com'),
('v2', NOW(), 'user 2', 'contact.html', '2.2.2', 'user 2 login',true, 'google.com'),
('v3', NOW(), 'user 3', 'about.html', '3.3.3', 'user 3 login',false, 'bing.com');
INSERT 0 3

dedp=# SELECT * FROM dedp.visits ORDER BY visit_id, event_time;
 visit_id |         event_time         | user_id |     page     |  ip   |    login     | is_connected | from_page  
----------+----------------------------+---------+--------------+-------+--------------+--------------+------------
 v1       | 2024-08-09 02:33:12.948959 | user 1  | home.html    | 1.1.1 | user 1 login | t            | google.com
 v2       | 2024-08-09 02:33:12.948959 | user 2  | contact.html | 2.2.2 | user 2 login | t            | google.com
 v3       | 2024-08-09 02:33:12.948959 | user 3  | about.html   | 3.3.3 | user 3 login | f            | bing.com
(3 rows)
```

After the operation we should have 3 new rows in the output table. Let's see now the consumer's code.

4. The new consumer relies on the _is_connected_ and _from_page_ columns to create a new table with connected users only.
For the sake of our example, we consider the consumer uses these two queries executed in separate transactions:

```
TRUNCATE TABLE dedp.connected_users_visits;

INSERT INTO dedp.connected_users_visits 
    SELECT visit_id, event_time, user_id, page, ip, login, from_page FROM dedp.visits
    WHERE is_connected = true AND from_page IS NOT NULL;
```

5. After running these two queries for the first time, the table should look like that:
```
dedp=# SELECT * FROM dedp.connected_users_visits;
 visit_id |         event_time         | user_id |     page     |  ip   |    login     | from_page  
----------+----------------------------+---------+--------------+-------+--------------+------------
 v1       | 2024-08-09 02:33:12.948959 | user 1  | home.html    | 1.1.1 | user 1 login | google.com
 v2       | 2024-08-09 02:33:12.948959 | user 2  | contact.html | 2.2.2 | user 2 login | google.com
(2 rows)
```

6. Let's change the schema with an incompatible way:

```
dedp=# ALTER TABLE dedp.visits RENAME COLUMN from_page TO referral;
ALTER TABLE

dedp=# CREATE TYPE user_details AS (
    id VARCHAR(25),
    ip VARCHAR(25),
    login VARCHAR(25),
    is_connected BOOL
);
CREATE TYPE

dedp=# ALTER TABLE dedp.visits ADD COLUMN user_info user_details;
ALTER TABLE

dedp=# ALTER TABLE dedp.visits DROP COLUMN user_id;
ALTER TABLE
dedp=# ALTER TABLE dedp.visits DROP COLUMN ip; 
ALTER TABLE
dedp=# ALTER TABLE dedp.visits DROP COLUMN login;
ALTER TABLE
dedp=# ALTER TABLE dedp.visits DROP COLUMN is_connected;
ALTER TABLE
```

After those changes, the table has the following schema:
```
dedp=# \d dedp.visits;
                            Table "dedp.visits"
   Column   |            Type             | Collation | Nullable | Default 
------------+-----------------------------+-----------+----------+---------
 visit_id   | character varying(10)       |           | not null | 
 event_time | timestamp without time zone |           | not null | 
 page       | character varying(25)       |           | not null | 
 referral   | character varying(25)       |           | not null | 
 user_info  | user_details                |           |          | 
Indexes:
    "visits_pkey" PRIMARY KEY, btree (visit_id, event_time)
```

7. Add new visits:
```
dedp=# INSERT INTO dedp.visits (visit_id, event_time, page, referral, user_info) VALUES 
('v1', NOW(), 'index.html', 'google.com', ROW('user 1', '1.1.1', 'user 1 login', true)),
('v2', NOW(), 'home.html', 'google.com', ROW('user 2', '2.2.2', 'user 2 login',true)),
('v3', NOW(), 'contact.html', 'bing.com', ROW('user 3',  '3.3.3', 'user 3 login',false));
INSERT 0 3

dedp=# SELECT * FROM dedp.visits;
 visit_id |         event_time         |     page     |  referral  |             user_info             
----------+----------------------------+--------------+------------+-----------------------------------
 v1       | 2024-08-09 02:33:12.948959 | home.html    | google.com | 
 v2       | 2024-08-09 02:33:12.948959 | contact.html | google.com | 
 v3       | 2024-08-09 02:33:12.948959 | about.html   | bing.com   | 
 v1       | 2024-08-09 02:59:20.003482 | index.html   | google.com | ("user 1",1.1.1,"user 1 login",t)
 v2       | 2024-08-09 02:59:20.003482 | home.html    | google.com | ("user 2",2.2.2,"user 2 login",t)
 v3       | 2024-08-09 02:59:20.003482 | contact.html | bing.com   | ("user 3",3.3.3,"user 3 login",f)
(6 rows)

```

8. Run the queries to recreate the connected users visits table:
```
dedp=# TRUNCATE TABLE dedp.connected_users_visits;

INSERT INTO dedp.connected_users_visits 
    SELECT visit_id, event_time, user_id, page, ip, login, from_page FROM dedp.visits
    WHERE is_connected = true AND from_page IS NOT NULL;
TRUNCATE TABLE
ERROR:  column "user_id" does not exist
LINE 2:     SELECT visit_id, event_time, user_id, page, ip, login, f...
                                         ^
HINT:  There is a column named "user_id" in table "connected_users_visits", but it cannot be referenced from this part of the query.
```

As you can see, the query failed but because of the `TRUNCATE` operation exected in a separate transaction, the table
is now empty:
```
dedp=# SELECT * FROM dedp.connected_users_visits;
 visit_id | event_time | user_id | page | ip | login | from_page 
----------+------------+---------+------+----+-------+-----------
(0 rows)
```

9. Let's recreate the visits table...
```
dedp=# DROP TABLE dedp.visits;
DROP TABLE
dedp=# CREATE TABLE dedp.visits (
    visit_id VARCHAR(10) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id VARCHAR(25) NOT NULL,
    page VARCHAR(25) NOT NULL,
    ip VARCHAR(25) NOT NULL,
    login VARCHAR(25) NOT NULL,
    is_connected BOOL NOT NULL DEFAULT false,
    from_page VARCHAR(25) NOT NULL,
    PRIMARY KEY(visit_id, event_time)
);
CREATE TABLE
dedp=# SELECT * FROM dedp.visits;
 visit_id | event_time | user_id | page | ip | login | is_connected | from_page 
----------+------------+---------+------+----+-------+--------------+-----------
(0 rows)
```

10. ...and follow the steps of the Schema migrator pattern. Let's start by adding new columns:
```

dedp=# ALTER TABLE dedp.visits ADD COLUMN user_info user_details;
ALTER TABLE

dedp=# ALTER TABLE dedp.visits ADD COLUMN referral VARCHAR(25) NOT NULL ;
ALTER TABLE
```

11. It's time to insert the first visits:
```
dedp=# INSERT INTO dedp.visits (visit_id, event_time, user_id, page, ip, login, is_connected, from_page, referral, user_info) VALUES 
('v1', NOW(), 'user 1', 'home.html', '1.1.1', 'user 1 login', true, 'google.com', 'google.com', ROW('user 1', '1.1.1', 'user 1 login', true)),
('v2', NOW(), 'user 2', 'contact.html', '2.2.2', 'user 2 login',true, 'google.com', 'google.com', ROW('user 2', '2.2.2', 'user 2 login',true)),
('v3', NOW(), 'user 3', 'about.html', '3.3.3', 'user 3 login',false, 'bing.com', 'bing.com', ROW('user 3',  '3.3.3', 'user 3 login',false));
INSERT 0 3

dedp=# SELECT * FROM dedp.visits;
 visit_id |         event_time         | user_id |     page     |  ip   |    login     | is_connected | from_page  |             user_info             |  referral  
----------+----------------------------+---------+--------------+-------+--------------+--------------+------------+-----------------------------------+------------
 v1       | 2024-08-09 03:08:02.207353 | user 1  | home.html    | 1.1.1 | user 1 login | t            | google.com | ("user 1",1.1.1,"user 1 login",t) | google.com
 v2       | 2024-08-09 03:08:02.207353 | user 2  | contact.html | 2.2.2 | user 2 login | t            | google.com | ("user 2",2.2.2,"user 2 login",t) | google.com
 v3       | 2024-08-09 03:08:02.207353 | user 3  | about.html   | 3.3.3 | user 3 login | f            | bing.com   | ("user 3",3.3.3,"user 3 login",f) | bing.com
(3 rows)
```

12. After running the queries to create the connected visits table, this time we should have two rows:
```
dedp=# TRUNCATE TABLE dedp.connected_users_visits;
TRUNCATE TABLE

dedp=# INSERT INTO dedp.connected_users_visits 
    SELECT visit_id, event_time, user_id, page, ip, login, from_page FROM dedp.visits
    WHERE is_connected = true AND from_page IS NOT NULL;
INSERT 0 2
dedp=# SELECT * FROM dedp.connected_users_visits;
 visit_id |         event_time         | user_id |     page     |  ip   |    login     | from_page  
----------+----------------------------+---------+--------------+-------+--------------+------------
 v1       | 2024-08-09 03:08:02.207353 | user 1  | home.html    | 1.1.1 | user 1 login | google.com
 v2       | 2024-08-09 03:08:02.207353 | user 2  | contact.html | 2.2.2 | user 2 login | google.com
(2 rows)
```

13. The connected users' visits table can now adapt to the new fields:
```
dedp=# TRUNCATE TABLE dedp.connected_users_visits;
TRUNCATE TABLE


dedp=# INSERT INTO dedp.connected_users_visits 
    SELECT visit_id, event_time, (user_info).id, page, (user_info).ip, (user_info).login, referral FROM dedp.visits
    WHERE (user_info).is_connected = true AND referral IS NOT NULL;
INSERT 0 2

dedp=# SELECT * FROM dedp.connected_users_visits;
 visit_id |         event_time         | user_id |     page     |  ip   |    login     | from_page  
----------+----------------------------+---------+--------------+-------+--------------+------------
 v1       | 2024-08-09 03:08:02.207353 | user 1  | home.html    | 1.1.1 | user 1 login | google.com
 v2       | 2024-08-09 03:08:02.207353 | user 2  | contact.html | 2.2.2 | user 2 login | google.com
(2 rows)
```

14. The producer, since the old columns are not used anymore, can safely drop them:
```

dedp=# ALTER TABLE dedp.visits DROP COLUMN user_id;
ALTER TABLE
dedp=# ALTER TABLE dedp.visits DROP COLUMN ip; 
ALTER TABLE
dedp=# ALTER TABLE dedp.visits DROP COLUMN login;
ALTER TABLE
dedp=# ALTER TABLE dedp.visits DROP COLUMN is_connected;
ALTER TABLE
dedp=# ALTER TABLE dedp.visits DROP COLUMN from_page;
ALTER TABLE

dedp=#  \d dedp.visits;
                            Table "dedp.visits"
   Column   |            Type             | Collation | Nullable | Default 
------------+-----------------------------+-----------+----------+---------
 visit_id   | character varying(10)       |           | not null | 
 event_time | timestamp without time zone |           | not null | 
 page       | character varying(25)       |           | not null | 
 user_info  | user_details                |           |          | 
 referral   | character varying(25)       |           | not null | 
Indexes:
    "visits_pkey" PRIMARY KEY, btree (visit_id, event_time)
```
# Static joiner - Slowly Changing Dimensions (SDC) for data at rest

## SDC Type 1
1. That's the easiest one as it overwrites the existing data.
2. Run the data generator and start a PostgreSQL instance:
```
cd docker
docker-compose down --volumes; docker-compose up
```
3. Perform the first join:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
SELECT v.visit_id, v.event_time, v.page, u.id, u.login, u.email FROM dedp.visits v  
JOIN dedp.users u ON u.id = v.user_id;

               visit_id               |     event_time      |    page    |                  id                  |        login         |                   email                   
--------------------------------------+---------------------+------------+--------------------------------------+----------------------+-------------------------------------------
 140492855753600_0                    | 2023-11-24 00:00:00 | contact    | 6ba4e640-bc25-4cd1-98f2-4dcaf3278390 | user_140492855753600 | user_140492855753600@abcdefghijklmnop.com
 140492855753600_1                    | 2023-11-24 00:00:00 | about      | eddb9515-6726-4233-a20b-289f6cddcbcd | user_140492855753601 | user_140492855753601@abcdefghijklmnop.com
 140492855753600_2                    | 2023-11-24 00:00:00 | contact    | 4bc6b068-c4f2-45fc-bbd8-57dbce03e2f1 | user_140492855753602 | user_140492855753602@abcdefghijklmnop.com
 140492855753600_3                    | 2023-11-24 00:00:00 | categories | 5cfcfa2d-2d1a-46b2-b56a-1bebd720e01d | user_140492855753603 | user_140492855753603@abcdefghijklmnop.com
 140492855753600_4                    | 2023-11-24 00:00:00 | about      | f0e76398-2a07-497c-b142-95f8d259c331 | user_140492855753604 | user_140492855753604@abcdefghijklmnop.com
(5 rows)
```
4. The SDC type 1 doesn't version the rows. Instead, it overwrites them in place. Let's do now an overwriting
operation for the users table:
```
UPDATE dedp.users SET email = NULL;
```

And if you run the query again, you'll see the same rows combined with users without email:
```
dedp=# SELECT v.visit_id, v.event_time, v.page, u.id, u.login, u.email FROM dedp.visits v  
JOIN dedp.users u ON u.id = v.user_id;
               visit_id               |     event_time      |    page    |                  id                  |        login         | email 
--------------------------------------+---------------------+------------+--------------------------------------+----------------------+-------
 140492855753600_0                    | 2023-11-24 00:00:00 | contact    | 6ba4e640-bc25-4cd1-98f2-4dcaf3278390 | user_140492855753600 | 
 140492855753600_1                    | 2023-11-24 00:00:00 | about      | eddb9515-6726-4233-a20b-289f6cddcbcd | user_140492855753601 | 
 140492855753600_2                    | 2023-11-24 00:00:00 | contact    | 4bc6b068-c4f2-45fc-bbd8-57dbce03e2f1 | user_140492855753602 | 
 140492855753600_3                    | 2023-11-24 00:00:00 | categories | 5cfcfa2d-2d1a-46b2-b56a-1bebd720e01d | user_140492855753603 | 
 140492855753600_4                    | 2023-11-24 00:00:00 | about      | f0e76398-2a07-497c-b142-95f8d259c331 | user_140492855753604 | 
(5 rows)
```

## SDC Type 2
1. The next SDC type introduces a versioning for the updated rows. The approach adds validity columns plus
a column indicating whether the given row is the current one.
2. Run the data generator and start a PostgreSQL instance:
```
cd docker
docker-compose down --volumes; docker-compose up
```
3. Add the validity and flag columns to the users table:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

ALTER TABLE dedp.users ADD COLUMN start_date TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE dedp.users ADD COLUMN end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31'::timestamp;
ALTER TABLE dedp.users ADD COLUMN is_current BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE dedp.users DROP CONSTRAINT users_pkey;
ALTER TABLE dedp.users ADD PRIMARY KEY (id, start_date);

SELECT id, email, start_date, end_date, is_current FROM dedp.users;
                  id                  |                   email                   |         start_date         |      end_date       | is_current 
--------------------------------------+-------------------------------------------+----------------------------+---------------------+------------
 47363a2f-cfd7-46d4-9d1d-fede1fe3acde | user_140339455318912@abcdefghijklmnop.com | 2024-02-25 03:38:56.925743 | 9999-12-31 00:00:00 | t
 a4557696-8b84-4c33-ab01-8480406e17cf | user_140339455318913@abcdefghijklmnop.com | 2024-02-25 03:38:56.925743 | 9999-12-31 00:00:00 | t
 5abbaccc-a079-413b-a86b-7d583027741b | user_140339455318914@abcdefghijklmnop.com | 2024-02-25 03:38:56.925743 | 9999-12-31 00:00:00 | t
 029f662e-3028-4dba-b85a-55b2ec267ba6 | user_140339455318915@abcdefghijklmnop.com | 2024-02-25 03:38:56.925743 | 9999-12-31 00:00:00 | t
 1865e5c5-e861-4c43-9fff-7ba5a802f1ce | user_140339455318916@abcdefghijklmnop.com | 2024-02-25 03:38:56.925743 | 9999-12-31 00:00:00 | t
(5 rows)

SELECT v.visit_id, v.event_time, v.page, u.id, u.login, u.email FROM dedp.visits v  
JOIN dedp.users u ON u.id = v.user_id AND NOW() BETWEEN start_date and end_date;

               visit_id               |     event_time      |    page    |                  id                  |        login         |                   email                   
--------------------------------------+---------------------+------------+--------------------------------------+----------------------+-------------------------------------------
 140339455318912_0                    | 2023-11-24 00:00:00 | index      | 47363a2f-cfd7-46d4-9d1d-fede1fe3acde | user_140339455318912 | user_140339455318912@abcdefghijklmnop.com
 140339455318912_1                    | 2023-11-24 00:00:00 | categories | a4557696-8b84-4c33-ab01-8480406e17cf | user_140339455318913 | user_140339455318913@abcdefghijklmnop.com
 140339455318912_2                    | 2023-11-24 00:00:00 | about      | 5abbaccc-a079-413b-a86b-7d583027741b | user_140339455318914 | user_140339455318914@abcdefghijklmnop.com
 140339455318912_3                    | 2023-11-24 00:00:00 | categories | 029f662e-3028-4dba-b85a-55b2ec267ba6 | user_140339455318915 | user_140339455318915@abcdefghijklmnop.com
 140339455318912_4                    | 2023-11-24 00:00:00 | contact    | 1865e5c5-e861-4c43-9fff-7ba5a802f1ce | user_140339455318916 | user_140339455318916@abcdefghijklmnop.com
(5 rows)
```
4. The SDC type 2 does version the rows. Therefore, setting the email to `NULL` as previously is not a good 
approach. Instead, the update should:
* "close" all active versions
* insert new active versions with empty email
```
UPDATE dedp.users SET end_date = NOW(), is_current = FALSE;
INSERT INTO dedp.users (id, login, email, registered_datetime, first_connection_datetime, last_connection_datetime, start_date) 
SELECT id, login, NULL, registered_datetime, first_connection_datetime, last_connection_datetime, end_date
FROM dedp.users;
```

At this moment, you should get new users inserted:
```
dedp=# SELECT id, email, start_date, end_date, is_current FROM dedp.users;
                  id                  |                   email                   |         start_date         |          end_date          | is_current 
--------------------------------------+-------------------------------------------+----------------------------+----------------------------+------------
 47363a2f-cfd7-46d4-9d1d-fede1fe3acde | user_140339455318912@abcdefghijklmnop.com | 2024-02-25 03:38:56.925743 | 2024-02-25 03:40:33.041762 | f
 a4557696-8b84-4c33-ab01-8480406e17cf | user_140339455318913@abcdefghijklmnop.com | 2024-02-25 03:38:56.925743 | 2024-02-25 03:40:33.041762 | f
 5abbaccc-a079-413b-a86b-7d583027741b | user_140339455318914@abcdefghijklmnop.com | 2024-02-25 03:38:56.925743 | 2024-02-25 03:40:33.041762 | f
 029f662e-3028-4dba-b85a-55b2ec267ba6 | user_140339455318915@abcdefghijklmnop.com | 2024-02-25 03:38:56.925743 | 2024-02-25 03:40:33.041762 | f
 1865e5c5-e861-4c43-9fff-7ba5a802f1ce | user_140339455318916@abcdefghijklmnop.com | 2024-02-25 03:38:56.925743 | 2024-02-25 03:40:33.041762 | f
 47363a2f-cfd7-46d4-9d1d-fede1fe3acde |                                           | 2024-02-25 03:40:33.041762 | 9999-12-31 00:00:00        | t
 a4557696-8b84-4c33-ab01-8480406e17cf |                                           | 2024-02-25 03:40:33.041762 | 9999-12-31 00:00:00        | t
 5abbaccc-a079-413b-a86b-7d583027741b |                                           | 2024-02-25 03:40:33.041762 | 9999-12-31 00:00:00        | t
 029f662e-3028-4dba-b85a-55b2ec267ba6 |                                           | 2024-02-25 03:40:33.041762 | 9999-12-31 00:00:00        | t
 1865e5c5-e861-4c43-9fff-7ba5a802f1ce |                                           | 2024-02-25 03:40:33.041762 | 9999-12-31 00:00:00        | t
(10 rows)
```

And if you issue the join query again, you should see it taking new version:
```
SELECT v.visit_id, v.event_time, v.page, u.id, u.login, u.email FROM dedp.visits v  
JOIN dedp.users u ON u.id = v.user_id AND NOW() BETWEEN start_date and end_date;
               visit_id               |     event_time      |    page    |                  id                  |        login         | email 
--------------------------------------+---------------------+------------+--------------------------------------+----------------------+-------
 140339455318912_0                    | 2023-11-24 00:00:00 | index      | 47363a2f-cfd7-46d4-9d1d-fede1fe3acde | user_140339455318912 | 
 140339455318912_1                    | 2023-11-24 00:00:00 | categories | a4557696-8b84-4c33-ab01-8480406e17cf | user_140339455318913 | 
 140339455318912_2                    | 2023-11-24 00:00:00 | about      | 5abbaccc-a079-413b-a86b-7d583027741b | user_140339455318914 | 
 140339455318912_3                    | 2023-11-24 00:00:00 | categories | 029f662e-3028-4dba-b85a-55b2ec267ba6 | user_140339455318915 | 
 140339455318912_4                    | 2023-11-24 00:00:00 | contact    | 1865e5c5-e861-4c43-9fff-7ba5a802f1ce | user_140339455318916 | 
(5 rows)
```

Since the data is versioned, you can also time-travel:
```

SELECT v.visit_id, v.event_time, v.page, u.id, u.login, u.email FROM dedp.visits v  
JOIN dedp.users u ON u.id = v.user_id AND '2024-02-25 03:38:59.977975+00'::timestamp BETWEEN u.start_date and u.end_date;
               visit_id               |     event_time      |    page    |                  id                  |        login         |                   email                   
--------------------------------------+---------------------+------------+--------------------------------------+----------------------+-------------------------------------------
 140339455318912_0                    | 2023-11-24 00:00:00 | index      | 47363a2f-cfd7-46d4-9d1d-fede1fe3acde | user_140339455318912 | user_140339455318912@abcdefghijklmnop.com
 140339455318912_1                    | 2023-11-24 00:00:00 | categories | a4557696-8b84-4c33-ab01-8480406e17cf | user_140339455318913 | user_140339455318913@abcdefghijklmnop.com
 140339455318912_2                    | 2023-11-24 00:00:00 | about      | 5abbaccc-a079-413b-a86b-7d583027741b | user_140339455318914 | user_140339455318914@abcdefghijklmnop.com
 140339455318912_3                    | 2023-11-24 00:00:00 | categories | 029f662e-3028-4dba-b85a-55b2ec267ba6 | user_140339455318915 | user_140339455318915@abcdefghijklmnop.com
 140339455318912_4                    | 2023-11-24 00:00:00 | contact    | 1865e5c5-e861-4c43-9fff-7ba5a802f1ce | user_140339455318916 | user_140339455318916@abcdefghijklmnop.com
(5 rows)
```

## SDC Type 4
1. I'm skipping the Type 3 on purpose. The Type 4 uses similar approach to the 2nd as it lets you freely time-travel
in the joins. However, instead of the current marker column, it uses a historical table.
2. Run the data generator and start a PostgreSQL instance:
```
cd docker
docker-compose down --volumes; docker-compose up
```
3. Create the historical table:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

CREATE TABLE dedp.users_history (
    id TEXT NOT NULL,
    login VARCHAR(45) NOT NULL,
    email VARCHAR(45) NULL,
    registered_datetime TIMESTAMP NOT NULL,
    first_connection_datetime TIMESTAMP NULL,
    last_connection_datetime TIMESTAMP NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL DEFAULT '9999-12-31'::timestamp,
    PRIMARY KEY(id, start_date)
);
```

And add there current rows to from the live data table:
```
INSERT INTO dedp.users_history 
SELECT *, NOW() FROM dedp.users;
```
4. If you update the users table, you need to repeat the operation from the SDC Type 2:
```
UPDATE dedp.users_history SET end_date = NOW();
UPDATE dedp.users SET email = NULL;
INSERT INTO dedp.users_history SELECT *, (SELECT MAX(end_date) FROM dedp.users_history)  FROM dedp.users;
```
5. Now you can join the rows from the current table (fast access):
```
SELECT v.visit_id, v.event_time, v.page, u.id, u.login, u.email FROM dedp.visits v  
JOIN dedp.users u ON u.id = v.user_id;
               visit_id               |     event_time      |    page     |                  id                  |        login         | email 
--------------------------------------+---------------------+-------------+--------------------------------------+----------------------+-------
 140430858189696_0                    | 2023-11-24 00:00:00 | contact     | d032f6bb-7bce-4da7-8dc7-46d09788857f | user_140430858189696 | 
 140430858189696_1                    | 2023-11-24 00:00:00 | home        | 61964c06-c37a-4ac5-9c13-dd8770ac9daf | user_140430858189697 | 
 140430858189696_2                    | 2023-11-24 00:00:00 | categories  | 97c274db-75a9-414c-b892-221d86835258 | user_140430858189698 | 
 140430858189696_3                    | 2023-11-24 00:00:00 | category_18 | ad043677-4851-4444-88dc-ce2245e08b73 | user_140430858189699 | 
 140430858189696_4                    | 2023-11-24 00:00:00 | home        | aaeac322-097c-4b58-b28b-5be08d56c2c3 | user_140430858189700 | 
(5 rows)
```

Or for historical reasons, query the old records:
```
SELECT v.visit_id, v.event_time, v.page, u.id, u.login, u.email FROM dedp.visits v  
JOIN dedp.users_history u ON u.id = v.user_id AND '2024-02-25 04:23:10.056545'::timestamp BETWEEN u.start_date and u.end_date;

               visit_id               |     event_time      |    page     |                  id                  |        login         |                   email                   
--------------------------------------+---------------------+-------------+--------------------------------------+----------------------+-------------------------------------------
 140430858189696_0                    | 2023-11-24 00:00:00 | contact     | d032f6bb-7bce-4da7-8dc7-46d09788857f | user_140430858189696 | user_140430858189696@abcdefghijklmnop.com
 140430858189696_1                    | 2023-11-24 00:00:00 | home        | 61964c06-c37a-4ac5-9c13-dd8770ac9daf | user_140430858189697 | user_140430858189697@abcdefghijklmnop.com
 140430858189696_2                    | 2023-11-24 00:00:00 | categories  | 97c274db-75a9-414c-b892-221d86835258 | user_140430858189698 | user_140430858189698@abcdefghijklmnop.com
 140430858189696_3                    | 2023-11-24 00:00:00 | category_18 | ad043677-4851-4444-88dc-ce2245e08b73 | user_140430858189699 | user_140430858189699@abcdefghijklmnop.com
 140430858189696_4                    | 2023-11-24 00:00:00 | home        | aaeac322-097c-4b58-b28b-5be08d56c2c3 | user_140430858189700 | user_140430858189700@abcdefghijklmnop.com
(5 rows)
```

## SDC Type 3
1. Finally, there is a Type 3 where the history is limited to past two versions only. The idea is to store them
in _current_ and _previous_ columns. As a result, our users table will have twice much columns as before.
2.  Run the data generator and start a PostgreSQL instance:
```
cd docker
docker-compose down --volumes; docker-compose up
```
3. Let's adapt the users table:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

ALTER TABLE dedp.users ADD COLUMN login_previous VARCHAR(45) NULL;
ALTER TABLE dedp.users ADD COLUMN email_previous VARCHAR(45) NULL;
ALTER TABLE dedp.users ADD COLUMN registered_datetime_previous TIMESTAMP NULL;
ALTER TABLE dedp.users ADD COLUMN first_connection_datetime_previous TIMESTAMP NULL;
ALTER TABLE dedp.users ADD COLUMN last_connection_datetime_previous TIMESTAMP NULL;
```
4. Now, updating the email moves all current values to the 'previous_' columns; we're moving all of them,
even the unchanged ones to get the whole consistent snapshot:
```
UPDATE dedp.users SET 
login_previous = login,
email_previous = email,
email = NULL,
registered_datetime_previous = registered_datetime,
first_connection_datetime_previous = first_connection_datetime,
last_connection_datetime_previous = last_connection_datetime;
```

You can query the table:
```
SELECT id, login, email, email_previous, login, login_previous FROM dedp.users;
                  id                  |        login         | email |              email_previous               |        login         |    login_previous    
--------------------------------------+----------------------+-------+-------------------------------------------+----------------------+----------------------
 e44e6a63-9086-4b0d-90c3-82a25e4155e0 | user_140476252478336 |       | user_140476252478336@abcdefghijklmnop.com | user_140476252478336 | user_140476252478336
 24101cc9-b41c-42eb-9905-3b02106a8259 | user_140476252478337 |       | user_140476252478337@abcdefghijklmnop.com | user_140476252478337 | user_140476252478337
 2f415202-a36f-413c-b293-e6e9da9425e3 | user_140476252478338 |       | user_140476252478338@abcdefghijklmnop.com | user_140476252478338 | user_140476252478338
 a772fa1e-112d-4821-8fde-b82f243bc28b | user_140476252478339 |       | user_140476252478339@abcdefghijklmnop.com | user_140476252478339 | user_140476252478339
 3bc002d7-d316-447e-ba97-edfe75468d0b | user_140476252478340 |       | user_140476252478340@abcdefghijklmnop.com | user_140476252478340 | user_140476252478340
(5 rows)
```

5. The join can decide what should be the returned column by selecting the current or the previous ones:
```
SELECT v.visit_id, v.event_time, v.page, u.id, u.login, u.email FROM dedp.visits v  
JOIN dedp.users u ON u.id = v.user_id;

               visit_id               |     event_time      |    page    |                  id                  |        login         | email 
--------------------------------------+---------------------+------------+--------------------------------------+----------------------+-------
 140476252478336_0                    | 2023-11-24 00:00:00 | contact    | e44e6a63-9086-4b0d-90c3-82a25e4155e0 | user_140476252478336 | 
 140476252478336_1                    | 2023-11-24 00:00:00 | about      | 24101cc9-b41c-42eb-9905-3b02106a8259 | user_140476252478337 | 
 140476252478336_2                    | 2023-11-24 00:00:00 | home       | 2f415202-a36f-413c-b293-e6e9da9425e3 | user_140476252478338 | 
 140476252478336_3                    | 2023-11-24 00:00:00 | category_9 | a772fa1e-112d-4821-8fde-b82f243bc28b | user_140476252478339 | 
 140476252478336_4                    | 2023-11-24 00:00:00 | category_9 | 3bc002d7-d316-447e-ba97-edfe75468d0b | user_140476252478340 | 
(5 rows)


SELECT v.visit_id, v.event_time, v.page, u.id, u.login_previous, u.email_previous FROM dedp.visits v  
JOIN dedp.users u ON u.id = v.user_id;

               visit_id               |     event_time      |    page    |                  id                  |    login_previous    |              email_previous               
--------------------------------------+---------------------+------------+--------------------------------------+----------------------+-------------------------------------------
 140476252478336_0                    | 2023-11-24 00:00:00 | contact    | e44e6a63-9086-4b0d-90c3-82a25e4155e0 | user_140476252478336 | user_140476252478336@abcdefghijklmnop.com
 140476252478336_1                    | 2023-11-24 00:00:00 | about      | 24101cc9-b41c-42eb-9905-3b02106a8259 | user_140476252478337 | user_140476252478337@abcdefghijklmnop.com
 140476252478336_2                    | 2023-11-24 00:00:00 | home       | 2f415202-a36f-413c-b293-e6e9da9425e3 | user_140476252478338 | user_140476252478338@abcdefghijklmnop.com
 140476252478336_3                    | 2023-11-24 00:00:00 | category_9 | a772fa1e-112d-4821-8fde-b82f243bc28b | user_140476252478339 | user_140476252478339@abcdefghijklmnop.com
 140476252478336_4                    | 2023-11-24 00:00:00 | category_9 | 3bc002d7-d316-447e-ba97-edfe75468d0b | user_140476252478340 | user_140476252478340@abcdefghijklmnop.com
(5 rows)
```
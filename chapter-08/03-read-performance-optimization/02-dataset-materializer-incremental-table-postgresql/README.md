# Read performance optimization - incremental table in PostgreSQL

1. Generate the datasets:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. The goal of this demo is to materialize the number of visits of each users:
```
$ docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

SELECT user_id, COUNT(*) FROM dedp.visits GROUP BY user_id;
```

The query returns nothing since there is no new data. Let's insert some rows first:
```
INSERT INTO dedp.visits (visit_id, event_time, user_id, page) VALUES 
('v1', '2024-10-10T08:00:00', 'user1', 'home'),
('v1', '2024-10-10T08:01:00', 'user1', 'contact'),
('v2', '2024-10-10T08:00:10', 'user2', 'home'),
('v3', '2024-10-10T08:00:05', 'user3', 'home');
```

The table should now return the 4 inserted rows:
```
SELECT * FROM dedp.visits;
               visit_id               |     event_time      | user_id |  page   |       insertion_time       
--------------------------------------+---------------------+---------+---------+----------------------------
 v1                                   | 2024-10-10 08:00:00 | user1   | home    | 2024-11-09 03:27:31.391883
 v1                                   | 2024-10-10 08:01:00 | user1   | contact | 2024-11-09 03:27:31.391883
 v2                                   | 2024-10-10 08:00:10 | user2   | home    | 2024-11-09 03:27:31.391883
 v3                                   | 2024-10-10 08:00:05 | user3   | home    | 2024-11-09 03:27:31.391883
(4 rows)
```

The aggregation query should also have some new data:
```
SELECT user_id, COUNT(*) FROM dedp.visits GROUP BY user_id;
 user_id | count 
---------+-------
 user3   |     1
 user2   |     1
 user1   |     2
(3 rows)
```

3. Let's create now a table materializing the aggregation:
```
CREATE TABLE dedp.visits_counter AS SELECT user_id, COUNT(*) FROM dedp.visits GROUP BY user_id;
SELECT 3

SELECT * FROM dedp.visits_counter;
 user_id | count 
---------+-------
 user3   |     1
 user2   |     1
 user1   |     2
(3 rows)

```

4. Let's suppose now there is some new data:
```
INSERT INTO dedp.visits (visit_id, event_time, user_id, page) VALUES 
('v1', '2024-10-10T08:02:00', 'user1', 'index'),
('v1', '2024-10-10T08:03:00', 'user1', 'about'),
('v2', '2024-10-10T08:00:44', 'user2', 'contact'),
('v3', '2024-10-10T08:00:45', 'user3', 'index');

SELECT * FROM dedp.visits;
               visit_id               |     event_time      | user_id |  page   |       insertion_time       
--------------------------------------+---------------------+---------+---------+----------------------------
 v1                                   | 2024-10-10 08:00:00 | user1   | home    | 2024-11-09 03:27:31.391883
 v1                                   | 2024-10-10 08:01:00 | user1   | contact | 2024-11-09 03:27:31.391883
 v2                                   | 2024-10-10 08:00:10 | user2   | home    | 2024-11-09 03:27:31.391883
 v3                                   | 2024-10-10 08:00:05 | user3   | home    | 2024-11-09 03:27:31.391883
 v1                                   | 2024-10-10 08:02:00 | user1   | index   | 2024-11-09 03:30:29.344377
 v1                                   | 2024-10-10 08:03:00 | user1   | about   | 2024-11-09 03:30:29.344377
 v2                                   | 2024-10-10 08:00:44 | user2   | contact | 2024-11-09 03:30:29.344377
 v3                                   | 2024-10-10 08:00:45 | user3   | index   | 2024-11-09 03:30:29.344377
(8 rows)
```

5. To avoid recomputing the whole view, we can use the `MERGE` command to combine existing count with the new visits:

```
MERGE INTO dedp.visits_counter AS target
USING (
    -- 2024-11-09T03:27:32 is the second after the previous insertion_time
    SELECT user_id, COUNT(*) AS visits FROM dedp.visits WHERE insertion_time > '2024-11-09T03:27:32' GROUP BY user_id
) AS input
ON target.user_id = input.user_id
WHEN MATCHED THEN 
    UPDATE SET count = count + input.visits 
WHEN NOT MATCHED THEN
    INSERT (user_id, count) VALUES (input.user_id, input.visits);

MERGE 3
```

As you can see, 3 rows have been impacted. Let's see the new results:
```
SELECT * FROM dedp.visits_counter;
 user_id | count 
---------+-------
 user3   |     2
 user2   |     2
 user1   |     4
(3 rows)
```

They're equal to the count executed on the whole dataset:
```
SELECT user_id, COUNT(*) AS visits FROM dedp.visits GROUP BY user_id;
 user_id | visits 
---------+--------
 user3   |      2
 user2   |      2
 user1   |      4
(3 rows)
```
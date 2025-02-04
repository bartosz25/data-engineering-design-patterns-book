# Access optimization - materialized view in PostgreSQL

1. Generate the datasets:
```
cd dataset
docker-compose down --volumes; docker-compose up
```

2. The goal of this demo is to optimize an aggregation operation that repeats very often. The query looks like this:
```
$ docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

SELECT 
   visit_id, ARRAY_AGG(CONCAT(position, ' -> ', page, '(', event_time , ')')) AS pages
FROM
(
   SELECT visit_id, 
   RANK() OVER (PARTITION BY visit_id ORDER BY event_time ASC) AS position,
   event_time, page
   FROM dedp.visits_all
) AS windowed_visits GROUP BY visit_id;
```

The aggregation results in a windowed visits such as:
```
               visit_id               |                                     pages                                     
--------------------------------------+-------------------------------------------------------------------------------
 140616707234688_0                    | {"1 -> main(2023-11-24 00:00:00)","2 -> categories(2023-11-24 00:02:00)"}
 140616707234688_1                    | {"1 -> page_13(2023-11-24 00:00:00)","2 -> contact(2023-11-24 00:05:00)"}
 140616707234688_10                   | {"1 -> index(2023-11-24 00:00:00)","2 -> contact(2023-11-24 00:02:00)"}
 140616707234688_100                  | {"1 -> category_18(2023-11-24 00:00:00)","2 -> page_12(2023-11-24 00:01:00)"}
 140616707234688_101                  | {"1 -> home(2023-11-24 00:00:00)","2 -> main(2023-11-24 00:04:00)"}
(5 rows)
```

Executing this query is costly in terms of aggregation operations:
```
dedp=# EXPLAIN SELECT 
   visit_id, ARRAY_AGG(CONCAT(position, ' -> ', page, '(', event_time , ')')) AS pages
FROM
(
   SELECT visit_id, 
   RANK() OVER (PARTITION BY visit_id ORDER BY event_time ASC) AS position,
   event_time, page
   FROM dedp.visits_all
) AS windowed_visits GROUP BY visit_id LIMIT 5;
                                       QUERY PLAN                                       
----------------------------------------------------------------------------------------
 Limit  (cost=237.66..238.10 rows=5 width=69)
   ->  GroupAggregate  (cost=237.66..325.16 rows=1000 width=69)
         Group Key: visits_all.visit_id
         ->  WindowAgg  (cost=237.66..277.66 rows=2000 width=60)
               ->  Sort  (cost=237.66..242.66 rows=2000 width=52)
                     Sort Key: visits_all.visit_id, visits_all.event_time
                     ->  Seq Scan on visits_all  (cost=0.00..128.00 rows=2000 width=52)
(7 rows)
```

3. Let's see now what happens when we create a materialized view:
```
CREATE MATERIALIZED VIEW dedp.windowed_visits
   AS 
      SELECT 
         visit_id, ARRAY_AGG(CONCAT(position, ' -> ', page, '(', event_time , ')')) AS pages
      FROM
      (
         SELECT visit_id, 
         RANK() OVER (PARTITION BY visit_id ORDER BY event_time ASC) AS position,
         event_time, page
         FROM dedp.visits_all
      ) AS windowed_visits GROUP BY visit_id
   WITH DATA;
```

The cost of querying the view is simply the cost of scanning a table:
```
dedp=# EXPLAIN SELECT * FROM dedp.windowed_visits;
                             QUERY PLAN                             
--------------------------------------------------------------------
 Seq Scan on windowed_visits  (cost=0.00..29.19 rows=819 width=180)
(1 row)
```

4. A drawback of the materialized view in PostgreSQL is the lack of automatic refresh. Let's add new rows
to the base table:

```
INSERT INTO dedp.visits_all (visit_id, event_time, user_id, page, context) 
    SELECT CONCAT('new_', visit_id), event_time, user_id, page, context FROM dedp.visits_all; 
```

Even though the query inserted new records, are still 1000 of the initial visits in the view:
```
dedp=# SELECT COUNT(*) FROM dedp.windowed_visits;
 count 
-------
  1000
(1 row)

dedp=# SELECT COUNT(*) FROM dedp.windowed_visits WHERE visit_id LIKE 'new_%';
 count 
-------
     0
(1 row)
```

5. To mitigate this issue, we need to refresh the view:
```
REFRESH MATERIALIZED VIEW dedp.windowed_visits WITH DATA;
```

Now, the view contains our new rows:
```
dedp=# SELECT COUNT(*) FROM dedp.windowed_visits;
 count 
-------
  2000
(1 row)

dedp=# SELECT COUNT(*) FROM dedp.windowed_visits WHERE visit_id LIKE 'new_%';
 count 
-------
  1000
(1 row)
```
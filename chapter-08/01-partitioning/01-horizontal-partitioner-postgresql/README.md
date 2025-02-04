# Partitioning - horizontal partitioner with PostgreSQL

1. Generate the datasets:
```
cd dataset
docker-compose down --volumes; docker-compose up
```

2. As you can see below, there are three tables with exactly the same records. Even though the querying can be simplified
with a `UNION`, there is no need of this construct if you create a partitioned table. 
```
$ docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

dedp=# SELECT COUNT(*) FROM dedp.visits_all_20231124;
 count 
-------
  1000
(1 row)

dedp=# SELECT COUNT(*) FROM dedp.visits_all_20231125;
 count 
-------
  1000
(1 row)

dedp=# SELECT COUNT(*) FROM dedp.visits_all_20231126;
 count 
-------
  1000
(1 row)

```

3. What is easy with PostgreSQL but more difficult to achieve with Apache Spark or Apache Kafka producers, is custom 
   partitioning logic. For the aforementioned tools, you'll need to implement your own abstractions while PostgreSQL provides
   some native partitioners natively. Let's see first the hash partitioner, hence the table partitioned with the same 
   default modulo hash algorithm as Apache Spark or Apache Kafka producers:
```
CREATE TABLE dedp.visits_all_hash (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    page VARCHAR(20) NULL,
    PRIMARY KEY(visit_id, event_time)
) PARTITION BY HASH(visit_id);

CREATE TABLE dedp.visits_all_hash_1 PARTITION OF dedp.visits_all_hash
    FOR VALUES WITH(MODULUS 3, REMAINDER 0);
CREATE TABLE dedp.visits_all_hash_2 PARTITION OF dedp.visits_all_hash
    FOR VALUES WITH(MODULUS 3, REMAINDER 1);
CREATE TABLE dedp.visits_all_hash_3 PARTITION OF dedp.visits_all_hash
    FOR VALUES WITH(MODULUS 3, REMAINDER 2);

INSERT INTO dedp.visits_all_hash (visit_id, event_time, user_id, page) SELECT visit_id, event_time, user_id, page FROM dedp.visits_all_20231124; 
INSERT INTO dedp.visits_all_hash (visit_id, event_time, user_id, page) SELECT visit_id, event_time, user_id, page FROM dedp.visits_all_20231125; 
INSERT INTO dedp.visits_all_hash (visit_id, event_time, user_id, page) SELECT visit_id, event_time, user_id, page FROM dedp.visits_all_20231126; 
```

Let's first pick 5 first visits and see at which partitions they landed!
```
dedp=# SELECT DISTINCT visit_id FROM dedp.visits_all_20231124 ORDER BY visit_id LIMIT 5; 
               visit_id               
--------------------------------------
 140077518007168_0                   
 140077518007168_1                   
 140077518007168_10                  
 140077518007168_100                 
 140077518007168_101                 
(5 rows)
```

Let's see first how many records are in each partition:
```
SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables
WHERE relname like 'visits_all_hash_%'
ORDER BY relname DESC ;

 schemaname |      relname      | n_live_tup 
------------+-------------------+------------
 dedp       | visits_all_hash_3 |        940
 dedp       | visits_all_hash_2 |       1034
 dedp       | visits_all_hash_1 |       1026
 dedp       | visits_all_hash   |          0
(4 rows)
```

Now, let's analyze the execution plan:

```
EXPLAIN SELECT * FROM dedp.visits_all_hash WHERE visit_id = '140077518007168_0';

                                         QUERY PLAN                                         
--------------------------------------------------------------------------------------------
 Bitmap Heap Scan on visits_all_hash_1 visits_all_hash  (cost=4.29..10.32 rows=2 width=105)
   Recheck Cond: (visit_id = '140077518007168_0'::bpchar)
   ->  Bitmap Index Scan on visits_all_hash_1_pkey  (cost=0.00..4.29 rows=2 width=0)
         Index Cond: (visit_id = '140077518007168_0'::bpchar)
(4 rows)
```

As you can see, it will read only one partition. What about another visit?

```
dedp=# EXPLAIN SELECT * FROM dedp.visits_all_hash WHERE visit_id = '140077518007168_100';
                                         QUERY PLAN                                         
--------------------------------------------------------------------------------------------
 Bitmap Heap Scan on visits_all_hash_2 visits_all_hash  (cost=4.29..10.32 rows=2 width=105)
   Recheck Cond: (visit_id = '140077518007168_100'::bpchar)
   ->  Bitmap Index Scan on visits_all_hash_2_pkey  (cost=0.00..4.29 rows=2 width=0)
         Index Cond: (visit_id = '140077518007168_100'::bpchar)
(4 rows)
```

This time the query engine will operate on a different table. The partitioning is then a great way to precisely
identify where a particular record live. It works that way for PostgreSQL but also for Big Data engines, such as BigQuery
or Delta Lake.

4. For the record, let's create tables with list partitions per browser:

```
CREATE TABLE dedp.visits_all_list (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    page VARCHAR(20) NULL,
    PRIMARY KEY(visit_id, event_time, page)
) PARTITION BY LIST(page);

CREATE TABLE dedp.visits_all_list_categories PARTITION OF dedp.visits_all_list
    FOR VALUES IN('categories');

CREATE TABLE dedp.visits_all_list_index PARTITION OF dedp.visits_all_list
    FOR VALUES IN('index');
    
CREATE TABLE dedp.visits_all_list_main PARTITION OF dedp.visits_all_list
    FOR VALUES IN('main');
    
CREATE TABLE dedp.visits_all_list_others PARTITION OF dedp.visits_all_list
    DEFAULT;
    
INSERT INTO dedp.visits_all_list (visit_id, event_time, user_id, page) SELECT visit_id, event_time, user_id, page FROM dedp.visits_all_20231124; 
INSERT INTO dedp.visits_all_list (visit_id, event_time, user_id, page) SELECT visit_id, event_time, user_id, page FROM dedp.visits_all_20231125; 
INSERT INTO dedp.visits_all_list (visit_id, event_time, user_id, page) SELECT visit_id, event_time, user_id, page FROM dedp.visits_all_20231126; 
```

Let's see now the data distribution:
```
SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables
WHERE relname like 'visits_all_list_%'
ORDER BY relname DESC ;

 schemaname |          relname           | n_live_tup 
------------+----------------------------+------------
 dedp       | visits_all_list_others     |       1908
 dedp       | visits_all_list_main       |        337
 dedp       | visits_all_list_index      |        370
 dedp       | visits_all_list_categories |        385
(4 rows)
```

And for the record, what will be the partition impacted by querying all visits for the main page?
```
EXPLAIN SELECT * FROM dedp.visits_all_list WHERE page = 'main';

                                       QUERY PLAN                                        
-----------------------------------------------------------------------------------------
 Seq Scan on visits_all_list_main visits_all_list  (cost=0.00..10.21 rows=337 width=103)
   Filter: ((page)::text = 'main'::text)
(2 rows)
```

5. Let's finish the example with range partitioning logic:

```
CREATE TABLE dedp.visits_all_range (
    visit_id CHAR(36) NOT NULL,
    event_time TIMESTAMP NOT NULL,
    user_id  TEXT NOT NULL,
    page VARCHAR(20) NULL,
    PRIMARY KEY(visit_id, event_time)
) PARTITION BY RANGE(event_time);

CREATE TABLE dedp.visits_all_range_20231124 PARTITION OF dedp.visits_all_range
    FOR VALUES FROM('2023-11-24 00:00:00') TO ('2023-11-24 23:59:59');

CREATE TABLE dedp.visits_all_range_20231125 PARTITION OF dedp.visits_all_range
    FOR VALUES FROM('2023-11-25 00:00:00') TO ('2023-11-25 23:59:59');
 
CREATE TABLE dedp.visits_all_range_20231126 PARTITION OF dedp.visits_all_range
    FOR VALUES FROM('2023-11-26 00:00:00') TO ('2023-11-26 23:59:59');

    
INSERT INTO dedp.visits_all_range (visit_id, event_time, user_id, page) SELECT visit_id, event_time, user_id, page FROM dedp.visits_all_20231124; 
INSERT INTO dedp.visits_all_range (visit_id, event_time, user_id, page) SELECT visit_id, event_time, user_id, page FROM dedp.visits_all_20231125; 
INSERT INTO dedp.visits_all_range (visit_id, event_time, user_id, page) SELECT visit_id, event_time, user_id, page FROM dedp.visits_all_20231126; 
```

What about the repartition? It should be even:
```
SELECT schemaname,relname,n_live_tup FROM pg_stat_user_tables
WHERE relname like 'visits_all_range_%'
ORDER BY relname DESC ;

 schemaname |          relname          | n_live_tup 
------------+---------------------------+------------
 dedp       | visits_all_range_20231126 |       1000
 dedp       | visits_all_range_20231125 |       1000
 dedp       | visits_all_range_20231124 |       1000
(3 rows)
```

And finally, the queries targeting all the partitions:
```
dedp=# EXPLAIN SELECT * FROM dedp.visits_all_range WHERE event_time = '2023-11-24';
                                          QUERY PLAN                                           
-----------------------------------------------------------------------------------------------
 Seq Scan on visits_all_range_20231124 visits_all_range  (cost=0.00..30.50 rows=500 width=105)
   Filter: (event_time = '2023-11-24 00:00:00'::timestamp without time zone)
(2 rows)

dedp=# EXPLAIN SELECT * FROM dedp.visits_all_range WHERE event_time = '2023-11-25';
                                          QUERY PLAN                                           
-----------------------------------------------------------------------------------------------
 Seq Scan on visits_all_range_20231125 visits_all_range  (cost=0.00..30.50 rows=500 width=105)
   Filter: (event_time = '2023-11-25 00:00:00'::timestamp without time zone)
(2 rows)

dedp=# EXPLAIN SELECT * FROM dedp.visits_all_range WHERE event_time = '2023-11-26';
                                          QUERY PLAN                                           
-----------------------------------------------------------------------------------------------
 Seq Scan on visits_all_range_20231126 visits_all_range  (cost=0.00..30.50 rows=500 width=105)
   Filter: (event_time = '2023-11-26 00:00:00'::timestamp without time zone)
(2 rows)
```
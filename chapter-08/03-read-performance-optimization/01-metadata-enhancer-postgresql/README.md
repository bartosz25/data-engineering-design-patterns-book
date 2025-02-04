# Metadata enhancer - PostgreSQL
1. Start the PostgreSQL database:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Let's connect to the database first:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
```

3. PostgreSQL has some underlying structures storing statistics about tables and columns. Let's see what's hidden
there for our dedp.visits table:

```
dedp=# SELECT n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup, last_analyze, last_autoanalyze FROM pg_stat_all_tables WHERE relname = 'visits';
 n_tup_ins | n_tup_upd | n_tup_del | n_live_tup | n_dead_tup | last_analyze |       last_autoanalyze        
-----------+-----------+-----------+------------+------------+--------------+-------------------------------
      1000 |         0 |         0 |       1000 |          0 |              | 2024-12-29 10:03:01.672883+00
(1 row)
```

This first statistics query stores the number of inserted, updated, and deleted rows, alongside all alive and dead tuples
and table analysis timestamps. In our case, the table was analyzed directly after ingesting the 1000 visits from the data
generator.

Let's see now what's hidden in another statistics table:
```
dedp=# SELECT attname, null_frac, n_distinct, most_common_vals, most_common_freqs FROM pg_stats WHERE tablename = 'visits';

  attname   | null_frac | n_distinct |                                                                                                                                                                                                         most_common_vals                                                                                                                                                                                                         |                                                                                                                               most_common_freqs                                                                                                                                
------------+-----------+------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 visit_id   |         0 |         -1 |                                                                                                                                                                                                                                                                                                                                                                                                                                  | 
 event_time |         0 |          1 | {"2024-12-24 00:00:00"}                                                                                                                                                                                                                                                                                                                                                                                                          | {1}
 user_id    |         0 |         -1 |                                                                                                                                                                                                                                                                                                                                                                                                                                  | 
 page       |         0 |         46 | {about,categories,home,index,contact,main,category_4,category_11,category_9,page_1,page_12,category_1,category_10,category_18,category_19,category_20,category_7,page_10,page_14,page_18,page_5,page_6,category_17,category_6,page_4,page_7,category_12,category_2,page_11,page_16,page_2,page_20,page_9,category_13,category_14,category_3,category_5,page_15,page_3,category_16,category_15,category_8,page_13,page_19,page_8} | {0.128,0.128,0.128,0.124,0.121,0.121,0.01,0.009,0.009,0.009,0.009,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.007,0.007,0.007,0.007,0.006,0.006,0.006,0.006,0.006,0.006,0.006,0.005,0.005,0.005,0.005,0.005,0.005,0.004,0.003,0.002,0.002,0.002,0.002}
 context    |         0 |         -1 |                                                                                                                                                                                                                                                                                                                                                                                                                                  | 
(5 rows)
```

Here we get the most common values per column alongside the distribution, plus number of distinct values whenever appropriate. 

4. To see how the statistics updated, let's insert new rows:

```
dedp=# CREATE TABLE dedp.visits_temporary AS SELECT * FROM dedp.visits;
INSERT INTO dedp.visits SELECT 
    CONCAT('new2_', visit_id), event_time, user_id, page, context
FROM dedp.visits_temporary;
SELECT 1000
INSERT 0 1000

dedp=# SELECT n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup, last_analyze, last_autoanalyze FROM pg_stat_all_tables WHERE relname = 'visits';
dedp=# SELECT n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup, last_analyze, last_autoanalyze FROM pg_stat_all_tables WHERE relname = 'visits';
 n_tup_ins | n_tup_upd | n_tup_del | n_live_tup | n_dead_tup | last_analyze |       last_autoanalyze        
-----------+-----------+-----------+------------+------------+--------------+-------------------------------
      2000 |         0 |         0 |       2000 |          0 |              | 2024-12-29 10:03:01.672883+00
(1 row)

```

As you can see, some entries were updated but the table hasn't been analyzed. The autoanalyze should be performed 
soonafter the insert statement. If you want, you can force the refresh by  issuing an `ANALYZE dedp.visits` command:

```
dedp=# ANALYZE VERBOSE dedp.visits
dedp-# ;
INFO:  analyzing "dedp.visits"
INFO:  "visits": scanned 107 of 107 pages, containing 2000 live rows and 0 dead rows; 2000 rows in sample, 2000 estimated total rows
ANALYZE
```

In that situation, you should see the _last_analyze_ column with a defined timestamp:

```
dedp=# SELECT n_tup_ins, n_tup_upd, n_tup_del, n_live_tup, n_dead_tup, last_analyze, last_autoanalyze FROM pg_stat_all_tables WHERE relname = 'visits';
 n_tup_ins | n_tup_upd | n_tup_del | n_live_tup | n_dead_tup |         last_analyze          |       last_autoanalyze        
-----------+-----------+-----------+------------+------------+-------------------------------+-------------------------------
      2000 |         0 |         0 |       2000 |          0 | 2024-12-29 10:04:10.105779+00 | 2024-12-29 10:04:01.742539+00
(1 row)
```

Now, the column statistics should also have some changes:


```
dedp=# SELECT attname, null_frac, n_distinct, most_common_freqs FROM pg_stats WHERE tablename = 'visits';

  attname   | null_frac | n_distinct |                                                                                                                                                                                                                                                                                                     most_common_freqs                                                                                                                                                                                                                                                                                                     
------------+-----------+------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 visit_id   |         0 |         -1 | 
 event_time |         0 |          1 | {1}
 user_id    |         0 |       -0.5 | {0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001}
 page       |         0 |         46 | {0.128,0.128,0.128,0.124,0.121,0.121,0.01,0.009,0.009,0.009,0.009,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.008,0.007,0.007,0.007,0.007,0.006,0.006,0.006,0.006,0.006,0.006,0.006,0.005,0.005,0.005,0.005,0.005,0.005,0.004,0.003,0.002,0.002,0.002,0.002,0.001}
 context    |         0 |       -0.5 | {0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001,0.001}
(5 rows)
```
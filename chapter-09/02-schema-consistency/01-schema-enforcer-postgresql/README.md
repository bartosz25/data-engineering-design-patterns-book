# Schema enforcer - PostgreSQL
1. Start the PostgreSQL database:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Let's connect to the database and see the schema of the dedp.visits table:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

dedp=# \d dedp.visits;
                             Table "dedp.visits"
    Column    |            Type             | Collation | Nullable | Default 
--------------+-----------------------------+-----------+----------+---------
 visit_id     | character varying(10)       |           | not null | 
 event_time   | timestamp without time zone |           | not null | 
 user_id      | character varying(25)       |           | not null | 
 page         | character varying(25)       |           | not null | 
 ip           | character varying(25)       |           | not null | 
 login        | character varying(25)       |           | not null | 
 is_connected | boolean                     |           | not null | false
 from_page    | character varying(25)       |           | not null | 
Indexes:
    "visits_pkey" PRIMARY KEY, btree (visit_id, event_time)
```

3. As you can see, the _page_ column is there. Let's try to remove it:
```
dedp=# ALTER TABLE dedp.visits DROP page;
ERROR:  Cannot remove page from dedp.visits!
CONTEXT:  PL/pgSQL function disallow_removing_page() line 10 at RAISE
```

The operation is not allowed. As you can see from the error, there is a `disallow_removing_page`
function raising some exception. We created this function as part of [the initialization script.sql](docker/init.sql):

```
CREATE FUNCTION disallow_removing_page() RETURNS event_trigger LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_event_trigger_dropped_objects() AS dropped_objects
    WHERE
      dropped_objects.object_type = 'table column' AND
      dropped_objects.object_identity = 'dedp.visits.page')
  THEN
    RAISE EXCEPTION 'Cannot remove page from dedp.visits!';
  END IF;
END $$;

CREATE EVENT TRIGGER page_removal ON sql_drop EXECUTE PROCEDURE disallow_removing_page();
```

The function runs before committing the column drop operation. As you can see, if a user tries to remove the 
_page_ column, the function will throw an exception.

4. Let's see if the exception exists if we try to remove a different column:
```
dedp=# ALTER TABLE dedp.visits DROP from_page;
ALTER TABLE
```

As you can see, the operation succeeded and if we check the table's schema, we should see the 
_from_page_ missing and the _page_ column still there:

```
dedp=# \d dedp.visits;
                             Table "dedp.visits"
    Column    |            Type             | Collation | Nullable | Default 
--------------+-----------------------------+-----------+----------+---------
 visit_id     | character varying(10)       |           | not null | 
 event_time   | timestamp without time zone |           | not null | 
 user_id      | character varying(25)       |           | not null | 
 page         | character varying(25)       |           | not null | 
 ip           | character varying(25)       |           | not null | 
 login        | character varying(25)       |           | not null | 
 is_connected | boolean                     |           | not null | false
Indexes:
    "visits_pkey" PRIMARY KEY, btree (visit_id, event_time)
```

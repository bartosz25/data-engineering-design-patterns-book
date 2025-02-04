# Fine-grained access control - columns in PostgreSQL

## Simple types
1. Run the data generator and start a PostgreSQL instance:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Create database users:
```
$ docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

CREATE USER user_a;
CREATE USER user_b;
```

3. Grant each user access on different columns of the _dedp.users_ table:
```
GRANT USAGE ON SCHEMA dedp TO user_a;
GRANT SELECT(id, login, registered_datetime, first_connection_datetime, last_connection_datetime) ON dedp.users TO user_a;
GRANT USAGE ON SCHEMA dedp TO user_b;
GRANT SELECT ON dedp.users TO user_b;
```

⚠️ For the sake of simplicity, we're testing here access per user. On production it's easier to manipulate user groups since the permissions scope will be then 
considerably smaller.

4. Connect to the database as _user_a_:
```
$ docker exec -ti dedp_postgresql psql --user user_a -d dedp

```

When the user tries to get all columns, hence including the email missing in the `GRANT` statement, the database will return an error:
```
dedp=> SELECT * FROM dedp.users;
ERROR:  permission denied for table users
```


Once correct columns selected, the user will be able to see the whole content:
```
dedp=> SELECT id, login FROM dedp.users;
                  id                  |        login         
--------------------------------------+----------------------
 43bb43f9-eeb1-4eba-b564-8cc58a9e34f1 | user_139624413977472
 48f1646b-1a84-4611-87cc-ee37a37bdd56 | user_139624413977473
 ecfe3d59-ae5f-4e1c-bff1-34b6c30d868d | user_139624413977474
 cce5aceb-de71-4ac6-baf9-5192c889c531 | user_139624413977475
 396b1e4a-ce5c-4784-aa5a-03642406b13e | user_139624413977476
(5 rows)
```



5. Connect to the database as _user_b_:
```
$ docker exec -ti dedp_postgresql psql --user user_b -d dedp
```

Here the `SELECT *` should work, as the user has an access granted to all the tables:
```
dedp=> SELECT * FROM dedp.users;
                  id                  |        login         |                   email                   |    registered_datetime     | first_connection_datetime  |  last_connection_datetime  
--------------------------------------+----------------------+-------------------------------------------+----------------------------+----------------------------+----------------------------
 43bb43f9-eeb1-4eba-b564-8cc58a9e34f1 | user_139624413977472 | user_139624413977472@abcdefghijklmnop.com | 2022-12-30 01:54:53.559756 |                            | 
 48f1646b-1a84-4611-87cc-ee37a37bdd56 | user_139624413977473 | user_139624413977473@abcdefghijklmnop.com | 2023-02-11 01:54:53.56075  |                            | 
 ecfe3d59-ae5f-4e1c-bff1-34b6c30d868d | user_139624413977474 | user_139624413977474@abcdefghijklmnop.com | 2023-06-29 01:54:53.561023 | 2023-07-02 11:03:53.561023 | 2024-04-28 04:56:53.561032
 cce5aceb-de71-4ac6-baf9-5192c889c531 | user_139624413977475 | user_139624413977475@abcdefghijklmnop.com | 2023-07-26 01:54:53.561288 | 2023-07-27 07:12:53.561288 | 2024-05-01 21:47:53.561296
 396b1e4a-ce5c-4784-aa5a-03642406b13e | user_139624413977476 | user_139624413977476@abcdefghijklmnop.com | 2023-07-01 01:54:53.561544 |                            | 
(5 rows)
```


## Complex types
1. Let's create now a table with a `STRUCT` type:

```
CREATE TYPE user_context AS (
  ip TEXT,
  login TEXT
);


CREATE TABLE dedp.visit_contexts (
  visit_id CHAR(36) NOT NULL,
  event_time TIMESTAMP NOT NULL,
  user_context user_context,
  PRIMARY KEY(visit_id, event_time)
);

INSERT INTO dedp.visit_contexts (visit_id, event_time, user_context) 
SELECT visit_id, event_time, user_context(context->'user'->'ip', context->'user'->'login')::user_context FROM dedp.visits;
```

2. Try to grant access to one attribute only:

```
GRANT SELECT((user_context).login) ON dedp.visit_contexts TO user_a;
```

The syntax is wrong:
```
ERROR:  syntax error at or near "("
LINE 1: GRANT SELECT((user_context).login) ON dedp.visit_contexts TO...
```

But it's the valid one for the `SELECT` queries:

```
dedp=# SELECT (user_context).ip FROM dedp.visit_contexts;

        ip         
-------------------
 "110.141.55.218"
 "173.240.218.135"
 "30.51.44.159"
 "16.124.246.57"
 "150.51.29.9"
 "15.224.199.125"
 "59.18.18.190"
...
```

Overall, it's not possible to define a per-attribute level access for the composite type columns.

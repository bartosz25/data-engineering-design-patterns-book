# Fine-grained access control - rows in PostgreSQL

1. Run the data generator and start a PostgreSQL instance:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. Create database users and grant full column access to them:
```
$ docker exec -ti dedp_postgresql psql --user dedp_test -d dedp


CREATE USER user_admin;
GRANT USAGE ON SCHEMA dedp TO user_admin;
GRANT SELECT ON dedp.users TO user_admin;
CREATE USER user_a;
GRANT USAGE ON SCHEMA dedp TO user_a;
GRANT SELECT ON dedp.users TO user_a;
CREATE USER user_b;
GRANT USAGE ON SCHEMA dedp TO user_b;
GRANT SELECT ON dedp.users TO user_b;
```

3. Create a row-level access policy:

* this one allows read and write access to all rows (`USING (true) WITH CHECK (true)`); put differently, the condition always evaluates to true if a user has this policy assigned
```
CREATE POLICY admin_all_access ON dedp.users TO user_admin USING (true) WITH CHECK (true);
```

* this policy only allows read access to the user of the same login (`USING (login = current_user)`)
```
CREATE POLICY user_row_access ON dedp.users USING (login = current_user);
```

💡 To control write queries, such as `INSERT`, `UPDATE`, and `MERGE`, use `WITH CHECK`.

4. Connect to the database as _user_a_:
```
$ docker exec -ti dedp_postgresql psql --user user_a -d dedp

```

When the user tries to get all columns, the database will only return the rows matching the condition from the `USING` clause:
```
dedp=> SELECT * FROM dedp.users;
    id     | login  |      email       
-----------+--------+------------------
 id_user_a | user_a | user_a@email.com
(1 row)
```


5. Connect to the database as _user_b_:
```
$ docker exec -ti dedp_postgresql psql --user user_b -d dedp
```
When the user tries to get all columns, the database will only return the rows matching the condition from the `USING` clause:
```
dedp=> SELECT * FROM dedp.users;
    id     | login  |      email       
-----------+--------+------------------
 id_user_b | user_b | user_b@email.com
(1 row)
```


6. Finally, let's connect as the _user_admin_:

```
$ docker exec -ti dedp_postgresql psql --user user_admin -d dedp
```

This time, since the row access condition always evaluates to `true`, the access is granted for all rows:
```
dedp=> SELECT * FROM dedp.users;
    id     | login  |      email       
-----------+--------+------------------
 id_user_a | user_a | user_a@email.com
 id_user_b | user_b | user_b@email.com
(2 rows)

```


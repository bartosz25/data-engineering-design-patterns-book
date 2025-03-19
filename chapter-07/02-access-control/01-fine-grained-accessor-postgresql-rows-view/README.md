# Fine-grained access control - rows in PostgreSQL via a view

> [!NOTE]
> This demos shows the view-based implementation of the pattern. However, you can also implement it with
> row-level access policies that you will find in the demo [../01-fine-grained-accessor-postgresql-rows](../01-fine-grained-accessor-postgresql-rows)

1. Run the data generator and start a PostgreSQL instance:
```
cd docker
docker-compose down --volumes; docker-compose up
```

2. While starting the database, we created the following table:
```
$ docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

dedp=# SELECT * FROM dedp.users;
    id     | login  |      email       
-----------+--------+------------------
 id_user_a | user_a | user_a@email.com
 id_user_b | user_b | user_b@email.com
(2 rows)
```

3. Let's create now the view that for each user will return only the row corresponding to his/her login, or 
all the rows if the connected user is a super user:
```
CREATE OR REPLACE VIEW dedp.users_view AS 
SELECT * FROM dedp.users WHERE login = current_user OR pg_has_role('dedp_test', 'USAGE');
```

4. To perform the tests, let's create new users:
```
CREATE USER user_a;
GRANT USAGE ON SCHEMA dedp TO user_a;
GRANT SELECT ON dedp.users_view TO user_a;
CREATE USER user_b;
GRANT USAGE ON SCHEMA dedp TO user_b;
GRANT SELECT ON dedp.users_view TO user_b;
```

5. Let's connect first as the _user_a_:
```
docker exec -ti dedp_postgresql psql --user user_a -d dedp

dedp=> SELECT * FROM dedp.users_view;
    id     | login  |      email       
-----------+--------+------------------
 id_user_a | user_a | user_a@email.com
(1 row)

```

As you can see, the user only sees his row. What about the user_b now?

```
docker exec -ti dedp_postgresql psql --user user_b -d dedp

dedp=> SELECT * FROM dedp.users_view;
    id     | login  |      email       
-----------+--------+------------------
 id_user_b | user_b | user_b@email.com
(1 row)
```

Same story. What if we connect now as the dedp_test who has super privileges?
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp

dedp=# SELECT * FROM dedp.users_view;
    id     | login  |      email       
-----------+--------+------------------
 id_user_a | user_a | user_a@email.com
 id_user_b | user_b | user_b@email.com
(2 rows)
```

This time we have access to both rows.
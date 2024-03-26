# Unaligned fan-in - partial aggregates with Apache Airflow 

1. Prepare the dataset:
```
cd docker
mkdir -p /tmp/dedp/ch06/02-fan-in/02-unaligned-fan-in-airflow-aggregates-partial/input
docker-compose down --volumes; docker-compose up
```

2. Start the Apache Airflow instance:
```
cd ../
./start.sh
```
3. Open the Apache Airflow UI and connect: http://localhost:8080 (dedp/dedp)
4. Explain the [visits_cube_generator.py](dags%2Fvisits_cube_generator.py)
* the pipeline performs parallel load of the hourly-based datasets
  * the difference with the aligned fan-in is that it runs the cube generation task whatever happens, i.e. 
    when all parent tasks has run independently on their outcome; Apache Airflow achieves that with the 
    `trigger_rule=TriggerRule.ALL_DONE` set on the _generate_cube_ task
5. Run the `visits_cube_generator`
6. Check the results in the table:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
dedp=# SELECT * FROM dedp.visits_cube;
 current_execution_time_id |    page     |                       user_id                        | is_approximate | visits_number 
---------------------------+-------------+------------------------------------------------------+----------------+---------------
 2024-02-01                |             |                                                      | t              |           170
 2024-02-01                | index       | 140100324367232_bb74f2d2-3b53-438c-9258-8a1eafa36232 | t              |             2
 2024-02-01                | page_3      | 140100324367232_bb74f2d2-3b53-438c-9258-8a1eafa36232 | t              |             1
 2024-02-01                | page_10     | 140100324367232_a98ed7e8-43c2-4bf6-8be9-41da7f53a7c5 | t              |             1
 2024-02-01                | category_8  | 140100324367232_a98ed7e8-43c2-4bf6-8be9-41da7f53a7c5 | t              |             1
 2024-02-01                | about       | 140100324367232_a98ed7e8-43c2-4bf6-8be9-41da7f53a7c5 | t              |             1
 2024-02-01                | category_10 | 140100324367232_bf8195e0-2558-475f-9780-5d41f679d0dc | t              |             1
 2024-02-01                | index       | 140100324367232_a98ed7e8-43c2-4bf6-8be9-41da7f53a7c5 | t              |             4
 2024-02-01                | page_9      | 140100324367232_db417591-b16e-499f-8259-6fba5ff31613 | t              |             1
 2024-02-01                | about       | 140100324367232_791be381-03c2-4086-ad31-caf4ddc10ec3 | t              |             4
 2024-02-01                | home        | 140100324367232_95a74b4a-00ef-4c75-99ac-8675b68ca386 | t              |             2
 2024-02-01                | page_12     | 140100324367232_bf8195e0-2558-475f-9780-5d41f679d0dc | t              |             1
 2024-02-01                | category_20 | 140100324367232_95a74b4a-00ef-4c75-99ac-8675b68ca386 | t              |             1
 2024-02-01                | page_12     | 140100324367232_a98ed7e8-43c2-4bf6-8be9-41da7f53a7c5 | t              |             1
 2024-02-01                | main        | 140100324367232_bf8195e0-2558-475f-9780-5d41f679d0dc | t              |             1
 2024-02-01                | contact     | 140100324367232_c5c5db92-09f8-4454-8ab4-c2a1263c2892 | t              |             2
 2024-02-01                | category_20 | 140100324367232_d67201a4-5aa9-4d60-85fc-53ade508ba0d | t              |             2
 2024-02-01                | main        | 140100324367232_95a74b4a-00ef-4c75-99ac-8675b68ca386 | t              |             1
 2024-02-01                | page_5      | 140100324367232_a98ed7e8-43c2-4bf6-8be9-41da7f53a7c5 | t              |             2
 2024-02-01                | contact     | 140100324367232_95a74b4a-00ef-4c75-99ac-8675b68ca386 | t              |             3
 2024-02-01                | categories  | 140100324367232_c5c5db92-09f8-4454-8ab4-c2a1263c2892 | t              |             4
 2024-02-01                | about       | 140100324367232_c5c5db92-09f8-4454-8ab4-c2a1263c2892 | t              |             2
 2024-02-01                | categories  | 140100324367232_791be381-03c2-4086-ad31-caf4ddc10ec3 | t              |             1
 2024-02-01                | categories  | 140100324367232_db417591-b16e-499f-8259-6fba5ff31613 | t              |             4
# ...
 2024-02-01                |             | 140100324367232_c5c5db92-09f8-4454-8ab4-c2a1263c2892 | t              |            17
 2024-02-01                |             | 140100324367232_a98ed7e8-43c2-4bf6-8be9-41da7f53a7c5 | t              |            17
 2024-02-01                |             | 140100324367232_bb74f2d2-3b53-438c-9258-8a1eafa36232 | t              |            17
 2024-02-01                |             | 140100324367232_bf8195e0-2558-475f-9780-5d41f679d0dc | t              |            17
 2024-02-01                |             | 140100324367232_d574824b-8a1a-41ad-ac02-a9cc8ff3e896 | t              |            17
 2024-02-01                |             | 140100324367232_b7c7bdf0-7839-48c2-9623-f8c06407c3a2 | t              |            17
 2024-02-01                |             | 140100324367232_791be381-03c2-4086-ad31-caf4ddc10ec3 | t              |            17
 2024-02-01                |             | 140100324367232_95a74b4a-00ef-4c75-99ac-8675b68ca386 | t              |            17
 2024-02-01                |             | 140100324367232_d67201a4-5aa9-4d60-85fc-53ade508ba0d | t              |            17
 2024-02-01                | page_12     |                                                      | t              |             2
 2024-02-01                | page_14     |                                                      | t              |             1
 2024-02-01                | category_20 |                                                      | t              |             4
 2024-02-01                | category_8  |                                                      | t              |             3
 2024-02-01                | page_5      |                                                      | t              |             2
 2024-02-01                | page_19     |                                                      | t              |             1
 2024-02-01                | categories  |                                                      | t              |            23
 2024-02-01                | page_15     |                                                      | t              |             2
 2024-02-01                | category_16 |                                                      | t              |             1
 2024-02-01                | category_5  |                                                      | t              |             1
 2024-02-01                | page_7      |                                                      | t              |             2
 2024-02-01                | page_10     |                                                      | t              |             2
 2024-02-01                | about       |                                                      | t              |            21
 2024-02-01                | category_1  |                                                      | t              |             2
 2024-02-01                | category_15 |                                                      | t              |             1
 2024-02-01                | category_10 |                                                      | t              |             2
 2024-02-01                | page_18     |                                                      | t              |             1
 2024-02-01                | page_6      |                                                      | t              |             1
 2024-02-01                | page_3      |                                                      | t              |             1
 2024-02-01                | category_3  |                                                      | t              |             1
 2024-02-01                | category_11 |                                                      | t              |             1
 2024-02-01                | page_2      |                                                      | t              |             2
 2024-02-01                | page_16     |                                                      | t              |             2
 2024-02-01                | category_13 |                                                      | t              |             1
(141 rows)
```
As you can see, the approximate flag is set to true since we don't have all hours to generate the final dataset.
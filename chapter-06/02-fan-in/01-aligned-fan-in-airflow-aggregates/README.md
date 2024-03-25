# Aligned fan-in - aggregates with Apache Airflow 

1. Prepare the dataset:
```
cd docker
mkdir -p /tmp/dedp/ch06/02-fan-in/01-aligned-fan-in-airflow-aggregates/input
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
  * you could include the loading step in the single step; however it would involve 
    some readability issues, such as :
    * not knowing which hour is not yet or what hour failed to load
    * having to customize many code snippets
  * with the aligned fan-in the design is much simpler and a single drawback is 
    the number of tasks to schedule (scheduling overhead)
5. Run the `visits_cube_generator`
6. Check the results in the table:
```
docker exec -ti dedp_postgresql psql --user dedp_test -d dedp
dedp=# SELECT * FROM dedp.visits_cube;

 current_execution_time_id |    page     |                       user_id                        | visits_number 
---------------------------+-------------+------------------------------------------------------+---------------
 2024-02-01                |             |                                                      |           290
 2024-02-01                | page_19     | 139835441728384_d6b2d7eb-6fcd-4254-9d95-2f7ee5e88e98 |             1
 2024-02-01                | categories  | 139835441728384_25fcfca3-10de-42e5-903b-7551f1b26105 |             3
 2024-02-01                | categories  | 139676157881216_d07f7292-8432-4009-b94e-cd5c5cb79740 |             1
 2024-02-01                | category_3  | 139676157881216_28a2fcc7-cb2a-40ae-b26f-b5797baa2280 |             1
 2024-02-01                | categories  | 139676157881216_54d3e338-7214-4b42-bbed-46af424c72f4 |             2
 2024-02-01                | page_16     | 139835441728384_cd9ba6c4-c888-4383-8121-bc0aa931a2a0 |             1
 2024-02-01                | about       | 139835441728384_d1ae4a75-98f4-4cd4-ad78-edbb23b05ef1 |             4
 2024-02-01                | category_5  | 139835441728384_cd9ba6c4-c888-4383-8121-bc0aa931a2a0 |             1
 2024-02-01                | page_4      | 139676157881216_ca3c4467-b074-444e-acf1-ac6c061715de |             1
 2024-02-01                | category_1  | 139835441728384_25fcfca3-10de-42e5-903b-7551f1b26105 |             1
 2024-02-01                | page_1      | 139835441728384_d6b2d7eb-6fcd-4254-9d95-2f7ee5e88e98 |             1
 2024-02-01                | about       | 139835441728384_a26d2c31-ef35-4fda-ba1b-a415296aa4c4 |             1
 2024-02-01                | about       | 139835441728384_e2ccfa54-e2e6-43f6-88de-f4540a9121ee |             4
 2024-02-01                | about       | 139835441728384_10ccebd1-3ba4-4562-916e-554a710eb755 |             2
 2024-02-01                | index       | 139835441728384_e2ccfa54-e2e6-43f6-88de-f4540a9121ee |             5
# ...
 2024-02-01                |             | 139835441728384_25fcfca3-10de-42e5-903b-7551f1b26105 |            24
 2024-02-01                |             | 139835441728384_cd9ba6c4-c888-4383-8121-bc0aa931a2a0 |            24
 2024-02-01                |             | 139835441728384_4e5af49b-be1f-4286-852f-ab6b483ed760 |            24
 2024-02-01                |             | 139676157881216_7b8864f7-16c1-4c52-a72e-b8a3098df4fe |             5
 2024-02-01                |             | 139676157881216_461c1a90-5ad7-46bd-bc6f-af4a812a3943 |             5
 2024-02-01                |             | 139835441728384_dafc112e-2c27-4fd8-ba84-2634a2718abe |            24
 2024-02-01                |             | 139835441728384_7ac41326-4bd3-4592-97bb-8cbe5bdd8c74 |            24
 2024-02-01                |             | 139676157881216_ca3c4467-b074-444e-acf1-ac6c061715de |             5
 2024-02-01                |             | 139835441728384_e2ccfa54-e2e6-43f6-88de-f4540a9121ee |            24
 2024-02-01                |             | 139835441728384_a26d2c31-ef35-4fda-ba1b-a415296aa4c4 |            24
 2024-02-01                |             | 139676157881216_3497f53c-39fe-4888-83f6-cda86b86ed27 |             5
 2024-02-01                |             | 139676157881216_3a516b5a-f078-47f8-a32f-42bfaf920d1a |             5
 2024-02-01                |             | 139676157881216_7fbbec8c-155c-4df6-9690-c519021d77e4 |             5
 2024-02-01                |             | 139676157881216_54d3e338-7214-4b42-bbed-46af424c72f4 |             5
 2024-02-01                |             | 139835441728384_d6b2d7eb-6fcd-4254-9d95-2f7ee5e88e98 |            24
 2024-02-01                |             | 139835441728384_d1ae4a75-98f4-4cd4-ad78-edbb23b05ef1 |            24
 2024-02-01                |             | 139835441728384_10ccebd1-3ba4-4562-916e-554a710eb755 |            24
 2024-02-01                |             | 139676157881216_9c60f52e-455c-46bf-a8df-d466b897004c |             5
 2024-02-01                |             | 139676157881216_d07f7292-8432-4009-b94e-cd5c5cb79740 |             5
 2024-02-01                | category_18 |                                                      |             1
 2024-02-01                | page_12     |                                                      |             3
 2024-02-01                | category_4  |                                                      |             1
 2024-02-01                | page_14     |                                                      |             2
 2024-02-01                | page_17     |                                                      |             2
 2024-02-01                | category_20 |                                                      |             1
 2024-02-01                | category_8  |                                                      |             3
 2024-02-01                | page_8      |                                                      |             1
 2024-02-01                | categories  |                                                      |            40
 2024-02-01                | page_19     |                                                      |             3
 2024-02-01                | category_9  |                                                      |             2
 2024-02-01                | page_1      |                                                      |             1
 2024-02-01                | page_15     |                                                      |             1
 2024-02-01                | category_17 |                                                      |             2
 2024-02-01                | category_16 |                                                      |             5
 2024-02-01                | page_2      |                                                      |             1
 2024-02-01                | page_16     |                                                      |             1
 2024-02-01                | category_6  |                                                      |             2
 2024-02-01                | category_2  |                                                      |             3
 2024-02-01                | category_13 |                                                      |             4
(218 rows)

```
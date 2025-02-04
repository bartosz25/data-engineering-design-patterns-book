# Pseudo-anonymizer - Apache Spark

1. Explain the [dataset_creator.py](dataset_creator.py)
* it creates a users dataset that looks like:

```
+-------+-------+--------------+------+
|user_id|country|           ssn|salary|
+-------+-------+--------------+------+
|      1| Poland|0940-0000-1000| 50000|
|      2| France|0469-0930-1000| 60000|
|      3|the USA|1230-0000-3940| 80000|
|      4|  Spain|8502-1095-9303| 52000|
+-------+-------+--------------+------+
```

2. Explain the [users_pseudo_anonymizer.py](users_pseudo_anonymizer.py)
* the pseudo-anonymization job with two types of transformations:
  * `mapInPandas` that applies row-wise
  * `withColumn` that applies per-column; we're using it since the _salary_ column changes type

3. Run the `dataset_creator.py`
4. Run the `users_pseudo_anonymizer.py`. It should generate the pseudo-anonymized dataset like:

```
+-------+-------+--------------+-----------+
|user_id|country|           ssn|     salary|
+-------+-------+--------------+-----------+
|      1|     eu|0***-0***-1***|    0-50000|
|      2|     eu|0***-0***-1***|50000-60000|
|      3|     na|1***-0***-3***|     60000+|
|      4|     eu|8***-1***-9***|50000-60000|
+-------+-------+--------------+-----------+
```
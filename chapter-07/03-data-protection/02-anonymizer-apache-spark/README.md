# Pseudo-anonymizer - Apache Spark

1. Explain the [dataset_creator.py](dataset_creator.py)
* it creates a users dataset that looks like:

```
+-------+----------+--------------------------+
|user_id|birthday  |email                     |
+-------+----------+--------------------------+
|1      |1980-01-20|work@contact.com          |
|2      |1985-01-20|dedp@waitingforcode.com   |
|3      |1990-01-20|contact@waitingforcode.com|
+-------+----------+--------------------------+
```

2. Explain the [users_anonymizer.py](users_anonymizer.py)
* the anonymization job with two types of transformations:
  * column removal with the `drop` function
  * value replacement with `withColumn` 

3. Run the `dataset_creator.py`
4. Run the `users_anonymizer.py`. It should generate the pseudo-anonymized dataset like:

```
+-------+-----------------------+
|user_id|email                  |
+-------+-----------------------+
|1      |brian44@example.com    |
|2      |rayroger@example.org   |
|3      |davidreilly@example.com|
+-------+-----------------------+
```

As the replacement column uses a random generation, you should see different emails than me.
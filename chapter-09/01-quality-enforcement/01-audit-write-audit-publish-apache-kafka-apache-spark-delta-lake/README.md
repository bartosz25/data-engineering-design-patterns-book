# Audit-Write-Audit-Publish - streaming job

1. Start Apache Kafka broker and generate the dataset:
```
rm -rf /tmp/dedp/ch09/01-quality-enforcement/01-audit-write-audit-publish-apache-kafka-apache-spark-delta-lake
mkdir -p /tmp/dedp/ch09/01-quality-enforcement/01-audit-write-audit-publish-apache-kafka-apache-spark-delta-lake/
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [tables_creator.py](tables_creator.py)
* for the demo we're creating two tables, one that will store valid datasets, and one that will store the invalid ones
* the validation scope here is a 15-minutes processing time window

3. Run the `tables_creator.py`

4. Explain the [visits_writer_job.py](visits_writer_job.py)
* the job performs two validation operations before considering the windowed dataset as valid or invalid:
  * volume check - static range validation
  * data quality check - simple null-based verification as the input data format is semi-structured JSON
* depending on the validation outcome, the job writes the rows to valid or error visits table
  * the errors table store the valid rows as well; it boils down to our perception of the audit scope which
    is tied to the whole window

5. Run the `visits_writer_job.py`

6. Stop the job after a while and start `tables_reader.py`. You should see some valid and invalid windows:

```
+--------+
|count(1)|
+--------+
|     147|
+--------+

+--------+
|count(1)|
+--------+
|    2147|
+--------+
```
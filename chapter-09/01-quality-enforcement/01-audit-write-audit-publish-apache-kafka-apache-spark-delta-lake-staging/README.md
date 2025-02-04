# Audit-Write-Audit-Publish - streaming job with a staging table

1. Start Apache Kafka broker and generate the dataset:
```
rm -rf /tmp/dedp/ch09/01-quality-enforcement/01-audit-write-audit-publish-apache-kafka-apache-spark-delta-lake-staging
mkdir -p /tmp/dedp/ch09/01-quality-enforcement/01-audit-write-audit-publish-apache-kafka-apache-spark-delta-lake-staging/
cd docker
docker-compose down --volumes; docker-compose up
```

2. Explain the [tables_creator.py](tables_creator.py)
* for the demo we're creating three tables
  * one that will store valid datasets, 
  * one that will store the invalid ones,
  * one that will store records to audit

3. Run the `tables_creator.py`

4. Explain the [visits_writer_job.py](visits_writer_job.py)
* the job doesn't validate anything; instead it writes each batch of records to the staging table

5. Run the `visits_writer_job.py`

6. Stop the `visits_writer_job.py` after few minutes.
 
7. Explain [staging_table_auditor_job.py](staging_table_auditor_job.py)
* ideally, this job should run at the same time than `visits_writer_job.py` but due to the Derby database 
concurrency limitations, it's not possible to do in our demo
* the job performs two validation operations before considering the windowed dataset as valid or invalid:
  * volume check - static range validation
  * data quality check - simple null-based verification as the input data format is semi-structured JSON
* depending on the validation outcome, the job writes the rows to valid or error visits table
  * the errors table store the valid rows as well; it boils down to our perception of the audit scope which
    is tied to the whole window

8. Start `staging_table_auditor_job.py`

9. Stop `staging_table_auditor_job.py` after few minutes.

10. Run `tables_reader.py`. You should see some count for records to audit, valid records, and finally the invalid ones:

```
-- Staging --
+--------+
|count(1)|
+--------+
|     441|
+--------+

-- Valid --
+--------+
|count(1)|
+--------+
|     923|
+--------+

-- Error --
+--------+
|count(1)|
+--------+
|    2155|
+--------+
```
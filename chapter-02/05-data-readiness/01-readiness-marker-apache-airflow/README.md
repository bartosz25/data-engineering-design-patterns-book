1. Start the Apache Airflow instance:
```
# enable the VirtualEnv before
./start.sh
```
2. Access the Web UI at http://localhost:8080/login/ with dedp/dedp as login/password
3. Enable the `dataset_creator` DAG.
* the DAG runs 3 tasks:
  * _delete_dataset_ that removes the dataset; useful in case of backfilling
  * _generate_dataset_ that creates a new dataset
  * _create_readiness_file_ that marks the dataset as being ready for processing by downstream consumers
![flow.png](assets%2Fflow.png)
4. Verify the readiness file:
```
tree /tmp/dedp/ch02/data-readiness/airflow/dataset

/tmp/dedp/ch02/data-readiness/airflow/dataset
├── COMPLETED
└── dataset.json

0 directories, 2 files
```
💡 You could also include the readiness file creation inside the dataset creation task.
# Orchestration - concurrent data ingestion

1. Generate the dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch06/04-orchestration/02-concurrent-airflow-data-ingestion/input
mkdir -p /tmp/dedp/ch06/04-orchestration/02-concurrent-airflow-data-ingestion/input-internal
docker-compose down --volumes; docker-compose up
```
2. Start Apache Airflow instance:
**⚠️ Enable the Virtual Environment before**
```
./start.sh
```
3. Access the Web UI at http://localhost:8080/login/ with dedp/dedp as login/password

4. Open the [devices_loader.py](dags%2Fdevices_loader.py)
* the pipeline can run concurrently in a safe manner because:
  * each run works on a dedicated output location
  * each run doesn't alter the input dataset which, for the purpose of this demo, is shared across the executions
* more specifically, the DAG is configured as:
  * running at most 5 different instances at the same time (`max_active_runs=5,`)
  * running tasks in parallel as they don't depend on each other (`'depends_on_past': False,`)

5. Enable the `devices_loader` on the UI. You should see multiple instances of the pipeline starting at the same time:
![concurrent_runs.png](assets%2Fconcurrent_runs.png)

The pipeline has two empty tasks only to better illustrate the parallel execution.
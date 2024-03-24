# Local sequencer - Airflow

1. Generate the dataset:
```
cd docker/dataset
mkdir -p /tmp/dedp/ch06/01-sequence/01-local-sequencer-airflow/input
docker-compose down --volumes; docker-compose up
```
2. Start a PostgreSQL instance:
```
cd ../postgresql
docker-compose down --volumes; docker-compose up
```
3. Start Apache Airflow instance:
**⚠️ Enable the Virtual Environment before**
```
./start.sh
```
4. Access the Web UI at http://localhost:8080/login/ with dedp/dedp as login/password
5. Explain the [devices_loader.py](dags%2Fdevices_loader.py)
* it's an example of an isolated pipeline where each step is responsible for one dedicated task
  * the purpose of each step is pretty easily understandable as the names tend to be self-explanatory
6. Explain the [devices_loader_not_isolated.py](dags%2Fdevices_loader_not_isolated.py)
* it's given here only as a counter-example; as you can see, only the sensor step is isolated
  * the query includes two tasks, loading data and refreshing view
    * it could make sense if you would like to run both in the same transaction
    * however, the transaction scope is not expected here and when you look at the pipeline from the UI
      (that's how you'll look at it daily!), you won't notice any particular details in this last task 
      which is bad for understanding
Below you can see a comparison of both graphs:

**Isolated**
![devices_loader_isolated.png](assets%2Fdevices_loader_isolated.png)

**Not isolated**
![devices_loader_not_isolated.png](assets%2Fdevices_loader_not_isolated.png)

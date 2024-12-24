# Dynamic late data integrator - trigger

## Data preparation
1. Generate datasets without the late data for the demo:
```
rm -rf /tmp/dedp/ch03/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-concurrent-trigger/dataset/devices
mkdir -p /tmp/dedp/ch03/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-concurrent-trigger/dataset/input
cd data-generator/dataset
docker-compose down --volumes; docker-compose up
```
2. Run the Delta table job for the first 7 partitions from the _data-generator_ project:
```
python load_devices_to_delta_table.py 2024-01-01
python load_devices_to_delta_table.py 2024-01-02
python load_devices_to_delta_table.py 2024-01-03
python load_devices_to_delta_table.py 2024-01-04
python load_devices_to_delta_table.py 2024-01-05
python load_devices_to_delta_table.py 2024-01-06
```
3. The dataset should look like in the following snippet:
```
tree /tmp/dedp/ch03/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-concurrent-trigger/dataset/devices -A

├── _delta_log
│   ├── 00000000000000000000.json
│   ├── 00000000000000000001.json
│   ├── 00000000000000000002.json
│   ├── 00000000000000000003.json
│   ├── 00000000000000000004.json
│   └── 00000000000000000005.json
├── event_time=2024-01-01
│   └── part-00000-71e50d8c-0a85-408d-8d09-747393e2cc99.c000.snappy.parquet
├── event_time=2024-01-02
│   └── part-00000-aed21cfb-9030-4d90-837b-6be902def9ab.c000.snappy.parquet
├── event_time=2024-01-03
│   └── part-00000-c21afbe7-7e00-4f48-bf43-81a6ea8a5615.c000.snappy.parquet
├── event_time=2024-01-04
│   └── part-00000-da96ad40-3886-486f-8209-51e15bcc3970.c000.snappy.parquet
├── event_time=2024-01-05
│   └── part-00000-e591e369-8ec5-4589-8047-e849b939f59e.c000.snappy.parquet
└── event_time=2024-01-06
    └── part-00000-78227a0e-403e-4c32-acf4-b618d2dd68b6.c000.snappy.parquet
```

## Job preparation
1. Build the job Docker image:
```
cd ../../late-data-integrator
docker run   -ti --rm --user $(id -u):$(id -g) -v $PWD:$PWD:rw  -w $PWD hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15  sbt clean assembly 

DOCKER_IMAGE_ARCHIVE_NAME=dedp_ch03_late_data_integrator_trigger.tar
docker build -t dedp_ch03_late_data_integrator_trigger .
docker save dedp_ch03_late_data_integrator_trigger:latest > $DOCKER_IMAGE_ARCHIVE_NAME
```
2. Install and start minikube (`minikube start`): https://minikube.sigs.k8s.io/docs/start/
```
minikube start
```

3. Mount the dataset directory volume:
```
minikube mount "/tmp/dedp/ch03/03-late-data/03-dynamic-late-data-integrator-apache-airflow-delta-lake-concurrent-trigger/dataset:/data_for_demo" --uid 185 --gid 185
```
The command also makes the local datasets available for the Spark jobs running on Kubernetes. We're using here the 
`mount` after starting to avoid some of the issues mentioned here: https://github.com/kubernetes/minikube/issues/13397

The mount also defines the Spark user from the Docker image. Otherwise, the job can't write files to the output directory.

⚠️ Do not stop this process. Otherwise, you'll lose access to the dataset.
4. Upload the Docker image:
```
DOCKER_IMAGE_ARCHIVE_NAME=dedp_ch03_late_data_integrator_trigger.tar
minikube image load $DOCKER_IMAGE_ARCHIVE_NAME
# check if the the image was correctly loaded
# You should see docker.io/library/dedp_ch03_late_data_integrator_trigger:latest
minikube image ls
```
5. Create the demo namespace and a service account: 
```
K8S_NAMESPACE=dedp-ch03-late-data-trigger
kubectl create namespace $K8S_NAMESPACE
kubectl config set-context --current --namespace=$K8S_NAMESPACE
kubectl create serviceaccount spark-editor
kubectl create rolebinding spark-editor-role --clusterrole=edit --serviceaccount=$K8S_NAMESPACE:spark-editor
```
6. Install `SparkOperator` from Helm:
```
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm install dedp-spark-operator spark-operator/spark-operator --namespace $K8S_NAMESPACE --version 1.1.27 --create-namespace --set webhook.enable=true  --set webhook.port=443
```
7. Start the K8S dashboard from a new tab 
```
minikube dashboard
``` 

## Pipeline
1. Explain the [devices_loader.py](airflow%2Fdags%2Fdevices_loader.py)
* the pipeline allows multiple execution except for 3 tasks:
  * _mark_partition_as_processing_ - because we want to have a sequential execution for the job that marks given partition
  as in progress to avoid concurrent scheduling
  * _late_data_detection_job_ - here we want to ensure that only one task triggers late partitions for backfilling; otherwise
    two tasks could detect the same past partitions for backfilling and trigger concurrent executions which we want to avoid
2. Start the Airflow instance:
```
./start.sh
```
3. Access the Web UI at http://localhost:8080/login/ with dedp/dedp as login/password
4. Start the DAG and wait for the last run. The DAG should progress with concurrent executions to the state present in the
last screenshot:

![expected_run_progress_1.png](assets/expected_run_progress_1.png)
![expected_run_progress_2.png](assets/expected_run_progress_2.png)
![expected_run_progress_3.png](assets/expected_run_progress_3.png)
![expected_run_result.png](assets/expected_run_result.png)



## Simulate late data
1. Return to the `data-generator` project and run again this command:
```
python load_devices_to_delta_table.py 2024-01-02
python load_devices_to_delta_table.py 2024-01-03
```
It should create late partition for an already processed DAG run.
2. Return to Apache Airflow UI to reprocess 2024-01-04 and 2024-01-05. Ensure to check the "Future" and "Downstream" boxes as in the next screenshot:

![clear_runs.png](assets/clear_runs.png)


3. Now, the restarted run should process its partition but also trigger the backfilling for the 
previously executed pipeline, plus the partitions with late data:

![backfilling_1.png](assets/backfilling_1.png)


![backfilling_late_data.png](assets/backfilling_late_data.png)

![backfilling_late_data_mapped_tasks.png](assets/backfilling_late_data_mapped_tasks.png)

4. The DAG should now look like:

![dag_after_backfilling_1.png](assets/dag_after_backfilling_1.png)


5. Stop minikube: `minikube stop`

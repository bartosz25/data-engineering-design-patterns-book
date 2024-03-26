# Parallel split - Apache Airflow jobs

1. Prepare the dataset:
```
cd docker
mkdir -p /tmp/dedp/ch06/03-fan-out/01-parallel-split-airflow-jobs/input
mkdir -p /tmp/dedp/ch06/03-fan-out/01-parallel-split-airflow-jobs/output
docker-compose down --volumes; docker-compose up
```

2. Prepare the PySpark job:
```
cd ../visits-loader-job
DOCKER_IMAGE_ARCHIVE_NAME=depd_visits_loader.tar
docker build -t depd_visits_loader .
docker save depd_visits_loader:latest > $DOCKER_IMAGE_ARCHIVE_NAME
```

3. Install and start minikube (`minikube start`): https://minikube.sigs.k8s.io/docs/start/
```
minikube start
```

4. Mount the dataset directory volume:
```
minikube mount "/tmp/dedp/ch06/03-fan-out/01-parallel-split-airflow-jobs:/data_for_demo" --uid 185 --gid 185
```
The command also makes the local datasets available for the Spark jobs running on Kubernetes. We're using here the 
`mount` after starting to avoid some of the issues mentioned here: https://github.com/kubernetes/minikube/issues/13397

The mount also defines the Spark user from the Docker image. Otherwise, the job can't write files to the output directory.

⚠️ Do not stop this process. Otherwise, you'll lose access to the dataset.

5. Upload the Docker image:
```
minikube image load $DOCKER_IMAGE_ARCHIVE_NAME
# check if the the image was correctly loaded
# You should see docker.io/library/depd_visits_loader:latest
minikube image ls
```
6. Create the demo namespace and a service account: 
```
K8S_NAMESPACE=dedp-ch06
kubectl create namespace $K8S_NAMESPACE
kubectl config set-context --current --namespace=$K8S_NAMESPACE
kubectl create serviceaccount spark-editor
kubectl create rolebinding spark-editor-role --clusterrole=edit --serviceaccount=$K8S_NAMESPACE:spark-editor
```
7. Install `SparkOperator` from Helm:
```
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install dedp-spark-operator spark-operator/spark-operator --namespace $K8S_NAMESPACE --create-namespace --set webhook.enable=true --set webhook.port=443
```
8. Start the K8S dashboard from a new tab 
```
minikube dashboard
```
9. Explain the [visits_loader.py](visits-loader-job%2Fvisits_loader.py)
* the job converts the input JSON file into one of two supported formats (CSV or Delta Lake)

10. Explain the [visits_converter.py](dags%2Fvisits_converter.py)
* the DAG creates 2 branches from the input file sensor
  * each branch is responsible for running a dedicated Spark job to convert the input data to different formats
  * typically we could do this in a single job; however, it would couple both formats meaning that the Delta and
  CSV consumers could start processing their respective formats only when both are created
    * however, a CSV reader doesn't care about the Delta table availability, and the opposite
  * also, coupling them would involve backfilling always both formats but it's not necessarily true as we might
    want to replay the CSV without impacting the Delta table
![parallel_split_graph.png](assets%2Fparallel_split_graph.png)
11. Start the Apache Airflow instance:
```
cd ../
./start.sh
```
12. Open the Apache Airflow UI and connect: http://localhost:8080 (dedp/dedp)
13. Enable the `visits_converter`
14. Check the results in the storage:
```
$ ls /tmp/dedp/ch06/03-fan-out/01-parallel-split-airflow-jobs/output/csv/event_date\=2024-02-01/
part-00000-05ceb229-8191-4162-bea8-f60d8e3d6b53.c000.csv  part-00000-470cadf8-4313-4b00-b9a2-cf5e0f72039c.c000.csv  part-00000-e8c8ed47-b31d-42d5-b272-6d55c8103f0b.c000.csv
part-00000-13650c60-916a-42cb-b65c-aa75308bb189.c000.csv  part-00000-c78981c6-de43-47ca-aed3-f0e535275d40.c000.csv

$ ls /tmp/dedp/ch06/03-fan-out/01-parallel-split-airflow-jobs/output/delta/_delta_log/
00000000000000000000.json  00000000000000000001.json  00000000000000000002.json  00000000000000000003.json
```
15. Stop the minikube:
```
minikube stop
```
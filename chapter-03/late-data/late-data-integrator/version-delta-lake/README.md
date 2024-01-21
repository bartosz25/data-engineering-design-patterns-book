# Incremental loader
## Data preparation
1. Generate the dataset for the demo:
```
cd dataset
mkdir -p /tmp/dedp/ch02/incremental_loader/
docker-compose down --volumes; docker-compose up
```

## PySpark job preparation
1. Build the job Docker image:
```
cd ../incremental-spark-job
DOCKER_IMAGE_ARCHIVE_NAME=depd_visits_loader.tar
docker build -t depd_visits_loader .
docker save depd_visits_loader:latest > $DOCKER_IMAGE_ARCHIVE_NAME
```
2. Install and start minikube (`minikube start`): https://minikube.sigs.k8s.io/docs/start/
```
minikube start
```

3. Mount the dataset directory volume:
```
minikube mount "/tmp/dedp/ch02/incremental_loader:/data_for_demo" --uid 185 --gid 185
```
The command also makes the local datasets available for the Spark jobs running on Kubernetes. We're using here the 
`mount` after starting to avoid some of the issues mentioned here: https://github.com/kubernetes/minikube/issues/13397

The mount also defines the Spark user from the Docker image. Otherwise, the job can't write files to the output directory.

⚠️ Do not stop this process. Otherwise, you'll lose access to the dataset.
4. Upload the Docker image:
```
minikube image load $DOCKER_IMAGE_ARCHIVE_NAME
# check if the the image was correctly loaded
# You should see docker.io/library/depd_visits_loader:latest
minikube image ls
```
5. Create the demo namespace and a service account: 
```
K8S_NAMESPACE=dedp-ch02
kubectl create namespace $K8S_NAMESPACE
kubectl config set-context --current --namespace=$K8S_NAMESPACE
kubectl create serviceaccount spark-editor
kubectl create rolebinding spark-editor-role --clusterrole=edit --serviceaccount=$K8S_NAMESPACE:spark-editor
```
6. Install `SparkOperator` from Helm:
```
helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
helm install dedp-spark-operator spark-operator/spark-operator --namespace $K8S_NAMESPACE --create-namespace --set webhook.enable=true --set webhook.port=443
```
7. Start the K8S dashboard from a new tab 
```
minikube dashboard
```
8. Explain the [visits_loader.py](incremental-spark-job%2Fvisits_loader.py)
* it uses the KISS approach to copy JSON files 
* input and output directories come from the data orchestrator; no orchestration-related logic in the job

## Orchestration layer
1. Explain the [visits_incremental_loader.py](airflow%2Fdags%2Fvisits_incremental_loader.py)
* the pipeline starts with a sensor waiting for the next partition to be available; 
  it's our readiness marker 
* next, the job DAG starts the data loading job and quits
* once the job submitted, it runs a sensor to check the job's outcome
💡 by using this `fire & forget` approach, the orchestration resources are freed and can be used for other tasks; otherwise 
the sensor would wait as long as the job didn't complete
2. Start the Apache Airflow instance:
```
cd airflow
./start.sh
```
3. Access the Web UI at http://localhost:8080/login/ with dedp/dedp as login/password
4. Enable the `visits_incremental_loader` DAG
![ch02_enable_dag.png](assets%2Fch02_enable_dag.png) 
5. Check the outcome:
```
$ tree /tmp/dedp/ch02/incremental_loader/output
```
6. Stop minikube: `minikube stop`

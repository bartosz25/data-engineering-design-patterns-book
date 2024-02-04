# Late data integrator
## Data preparation
1. Generate datasets without the late data for the demo:
```
rm -rf /tmp/dedp/ch03/late-data-integrator/dataset/devices
mkdir -p /tmp/dedp/ch03/late-data-integrator/dataset/input
cd data-generator/dataset
docker-compose down --volumes; docker-compose up
```
2. Run the Delta table job for the first 4 partitions from the _data-generator_ project:
```
python load_devices_to_delta_table.py 2024-01-01
python load_devices_to_delta_table.py 2024-01-02
python load_devices_to_delta_table.py 2024-01-03
```
3. The dataset should look like in the following snippet:
```
$ tree -A
.
└── dataset
    ├── backfilling_configuration.json
    ├── devices
    │   ├── _delta_log
    │   │   ├── 00000000000000000000.json
    │   │   ├── 00000000000000000001.json
    │   │   └── 00000000000000000002.json
    │   ├── event_time=2024-01-01
    │   │   └── part-00000-4a98eec3-fc20-4c2e-bbef-e9fde6d9e29e.c000.snappy.parquet
    │   ├── event_time=2024-01-02
    │   │   └── part-00000-d117245e-f795-48c0-8d80-4c6ba9c663a0.c000.snappy.parquet
    │   └── event_time=2024-01-03
    │       └── part-00000-16d6a09f-85a8-4de3-83cd-7aa4ab66d72a.c000.snappy.parquet
    └── input
        └── dataset.json

7 directories, 8 files
```

## Job preparation
1. Build the job Docker image:
```
cd backfilling-configurator
docker run  -v ~/.m2:/var/maven/.m2:rw -ti --rm --user $(id -u):$(id -g) -e MAVEN_CONFIG=/var/maven/.m2 -v $PWD:$PWD:rw  -w $PWD maven:3.9.3-amazoncorretto-11-debian  mvn -Duser.home=/var/maven install

DOCKER_IMAGE_ARCHIVE_NAME=dedp_ch03_backfilling_configurator.tar
docker build -t dedp_ch03_backfilling_configurator .
docker save dedp_ch03_backfilling_configurator:latest > $DOCKER_IMAGE_ARCHIVE_NAME
```
2. Install and start minikube (`minikube start`): https://minikube.sigs.k8s.io/docs/start/
```
minikube start
```

3. Mount the dataset directory volume:
```
minikube mount "/tmp/dedp/ch03/late-data-integrator/dataset:/data_for_demo" --uid 185 --gid 185
```
The command also makes the local datasets available for the Spark jobs running on Kubernetes. We're using here the 
`mount` after starting to avoid some of the issues mentioned here: https://github.com/kubernetes/minikube/issues/13397

The mount also defines the Spark user from the Docker image. Otherwise, the job can't write files to the output directory.

⚠️ Do not stop this process. Otherwise, you'll lose access to the dataset.
4. Upload the Docker image:
```
minikube image load $DOCKER_IMAGE_ARCHIVE_NAME
# check if the the image was correctly loaded
# You should see docker.io/library/dedp_ch03_backfilling_configurator:latest
minikube image ls
```
5. Create the demo namespace and a service account: 
```
K8S_NAMESPACE=dedp-ch03
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

## Pipeline
1. Explain the [devices_loader.py](airflow%2Fdags%2Fdevices_loader.py)
* the job implements the pattern via two parallel branches
  * one to process the data
  * one to generate the backfilling configuration
* upon processing the data, the job updates the last processed version and triggers backfilling for past partitions
  if some of them received late data
2. Start the Airflow instance:
```
./start.sh
```
3. Access the Web UI at http://localhost:8080/login/ with dedp/dedp as login/password
4. Start the DAG and wait for the last run. The DAG should look like this:
![expected_run_1.png](assets%2Fexpected_run_1.png)

## Simulate late data
1. Return to the `data-generator` project and run again this command:
```
 python load_devices_to_delta_table.py 2024-01-01
```
It should create late partition for an already processed DAG run.
2. Return to Apache Airflow UI and reprocess the most recent task:
![clear_dag_run.png](assets%2Fclear_dag_run.png)
3. Now, the restarted run should process its partition but also trigger the backfilling for the 
previously executed pipeline:
![backfilling_late_data.png](assets%2Fbackfilling_late_data.png)

4. Verify the last processed version file and the most recent backfilling configuration:
```
# last processed version
$ cat /tmp/_last_processed_version
3

# most recent backfilling configuration (nothing to reprocess as it comes from the 2024-01-01 backfilled run)
$ cat /tmp/dedp/ch03/late-data-integrator/dataset/backfilling_configuration.json

{"partitions":[],"lastProcessedVersion":3}
```

5. The DAG should now look like:

![expected_dag_backfilling.png](assets%2Fexpected_dag_backfilling.png)
6. Stop minikube: `minikube stop`

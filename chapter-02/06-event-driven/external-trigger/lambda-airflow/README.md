1. First, retrieve your internal IP of and put paste it to the `trigger-lambda/event_handler.py` file 
under my_ip variable. Example for my IP:
```
my_ip = '192.168.1.55'
```
💡 For Ubuntu you can get it from https://help.ubuntu.com/stable/ubuntu-help/net-findip.html.en, for MacOS from https://www.security.org/vpn/find-mac-ip-address/
   and for Windows from https://support.microsoft.com/en-us/windows/find-your-ip-address-in-windows-f21a9bbc-c582-55cd-35e0-73431160a1b9
2. You don't need an AWS subscription for this demo. We're going to simulate the services with [Localstack](https://www.localstack.cloud/) project.
   The project should be installed when you create your Virtual Environment. It's defined in the `requirements.txt`:
```
LAMBDA_DOCKER_NETWORK=host
localstack start -d
```
3. Generate the devices dataset:
```
cd dataset
mkdir -p /tmp/dedp/ch02/event-driven/external-trigger/input/
docker-compose down --volumes; docker-compose up
```
4. Prepare the Lambda package for deployment:
```
cd ../trigger-lambda
mkdir package
pip install --target ./package requests==2.31.0 boto3==1.33.8
cd package
zip -r ../my_deployment_package.zip .
cd ..
zip my_deployment_package.zip event_handler.py
cd ..
```
5. Initialize the local AWS stack:
```
# Create the bucket first
awslocal s3api create-bucket --bucket devices

# Declare the Lambda function
aws lambda create-function \
--endpoint-url http://localhost:4566 \
--function-name devices-loader-trigger \
--runtime python3.8 \
--handler event_handler.lambda_handler \
--region us-east-1 \
--zip-file fileb://trigger-lambda/my_deployment_package.zip \
--role arn:aws:iam::123456789000:role/ignoreme

# Configure the Lambda S3 trigger
aws s3api put-bucket-notification-configuration --bucket devices --notification-configuration file://s3hook.json --endpoint-url http://localhost:4566
```
If you see an error like _An error occurred (InvalidArgument) when calling the PutBucketNotificationConfiguration operation: Unable to validate the following destination configurations_
you may need to retry the last command.
6. Start the Airflow instance:
```
cd airflow
./start.sh
```
7. Access the Web UI at http://localhost:8080/login/ with dedp/dedp as login/password
8. Enable the `devices_loader` DAG:
![enable_dag.png](assets%2Fenable_dag.png)
9. Upload the devices dataset to the bucket:
```
awslocal s3api put-object \
  --bucket devices \
  --key /tmp/dedp/ch02/event-driven/external-trigger/input/dataset.json \
  --body /tmp/dedp/ch02/event-driven/external-trigger/input/dataset.json
```
💡 For the sake of simplicity, the key of the uploaded object follows its path on the local file system.

10. Open the Airflow UI again. You should see the `devices_loader` DAG running:
![running_dag.png](assets%2Frunning_dag.png)
11. Once the run completes, verify if the loaded file is present in the output directory:
```
$ tree  /tmp/dedp/ch02/event-driven/external-trigger/output/
/tmp/dedp/ch02/event-driven/external-trigger/output/
└── devices_5213e115-840e-481d-a474-ebd43e0d423a.json

0 directories, 1 file

$ head  /tmp/dedp/ch02/event-driven/external-trigger/output/devices_5213e115-840e-481d-a474-ebd43e0d423a.json 
{"type": "mac", "full_name": "MacBook Air (15-inch, M2, 2023)", "version": "macOS Ventura"}
{"type": "galaxy", "full_name": "Galaxy Gio", "version": "Android 14"}
{"type": "galaxy", "full_name": "Galaxy Nexus", "version": "Android 15"}
{"type": "mac", "full_name": "MacBook Pro (13-inch, M1, 2020)", "version": "macOS Sonoma"}
{"type": "lenovo", "full_name": "ThinkBook 16 Gen 6 (16\" Intel) Laptop", "version": "Ubuntu 23"}
{"type": "lg", "full_name": "Nexus 4", "version": "Android 12"}
{"type": "htc", "full_name": "Amaze 4g", "version": "Android 13"}
{"type": "galaxy", "full_name": "Galaxy Gio", "version": "Android 13"}
{"type": "htc", "full_name": "Sensation Xe", "version": "Android 14"}
{"type": "lenovo", "full_name": "ThinkPad X1 Carbon Gen 10 (14\" Intel) Laptop", "version": "Ubuntu 20"}
```
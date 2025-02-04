# Secretless connector - Apache Spark and PostgreSQL

1. Extract the keys from a PostgreSQL instance:
```
docker run  -ti postgres:15 bin/bash

cd certs
docker container cp 19d0802aa12f:/etc/ssl/private/ssl-cert-snakeoil.key ./
docker container cp 19d0802aa12f:/etc/ssl/certs/ssl-cert-snakeoil.pem ./
```
⚠️ You could also generate your own certificates but there are multiple issues you might need to deal with, such as
different OpenSSL versions or access issues. Copying the certificates is currently the easiest solution I found.

 
2. Generate the dataset:
```
cd dataset
docker-compose down --volumes; docker-compose up
```

3. Explain the [devices_json_converter.py](devices_json_converter.py)
* the job simply coverts PostgreSQL records to JSON files 
* the difference with a classical job is the lack of password
  * instead, the job relies on the certificates defined in the `properties`

4. Run the `devices_json_converter.py`. It should read PostgreSQL data and write it as JSON files.
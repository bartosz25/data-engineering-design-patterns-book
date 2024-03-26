#!/usr/bin/env bash
airflow db reset
airflow db migrate
airflow users create --username "dedp" --role "Admin" --password "dedp" --email "empty" --firstname "admin" --lastname "admin"
export AIRFLOW__CORE__DAGS_FOLDER=./dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__WEBSERVER__WORKERS=2
airflow connections delete docker_postgresql
airflow connections add --conn-host localhost --conn-type postgres --conn-login dedp_test --conn-password dedp_test --conn-port 5432 docker_postgresql
airflow connections delete docker_postgresql_uri
airflow connections add --conn-uri postgresql://dedp_test:dedp_test@localhost:5432/dedp docker_postgresql_uri
airflow webserver & airflow scheduler
#!/usr/bin/env bash
airflow db reset
airflow db init
airflow users create --username "dedp" --role "Admin" --password "dedp" --email "empty" --firstname "admin" --lastname "admin"
export AIRFLOW__CORE__PLUGINS_FOLDER=./plugins
export AIRFLOW__CORE__DAGS_FOLDER=./dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false
airflow connections delete docker_postgresql
airflow connections add --conn-host localhost --conn-type postgres --conn-login dedp_test --conn-password dedp_test --conn-port 5432 docker_postgresql
airflow webserver & airflow scheduler
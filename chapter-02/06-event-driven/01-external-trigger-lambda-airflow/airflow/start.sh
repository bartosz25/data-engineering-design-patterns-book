#!/usr/bin/env bash
export AIRFLOW_HOME=~/airflow-2.7.3
airflow db reset
airflow db init
airflow users create --username "dedp" --role "Admin" --password "dedp" --email "empty" --firstname "admin" --lastname "admin"
export AIRFLOW__CORE__PLUGINS_FOLDER=./plugins
export AIRFLOW__CORE__DAGS_FOLDER=./dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
airflow connections delete docker_postgresql
airflow connections add --conn-host localhost --conn-type postgres --conn-login dedp_test --conn-password dedp_test --conn-port 5432 docker_postgresql
airflow webserver & airflow scheduler
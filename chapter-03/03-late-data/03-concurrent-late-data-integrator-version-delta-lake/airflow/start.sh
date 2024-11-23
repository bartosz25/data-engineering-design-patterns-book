#!/usr/bin/env bash
airflow db reset
airflow db init
airflow users create --username "dedp" --role "Admin" --password "dedp" --email "empty" --firstname "admin" --lastname "admin"
export AIRFLOW__CORE__DAGS_FOLDER=./dags
export AIRFLOW__CORE__LOAD_EXAMPLES=false
export AIRFLOW__WEBSERVER__WORKERS=2
airflow webserver & airflow scheduler
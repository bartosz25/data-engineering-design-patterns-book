# Secrets pointer - Apache Spark, PostgreSQL, and Secrets Manager

1. Import all dependencies from `requirements.txt` and start Localstack:
```
LAMBDA_DOCKER_NETWORK=host
localstack start -d
```

2. Create the login and password secerts:
```
awslocal secretsmanager create-secret \
    --name psql_user \
    --description "User for PostgreSQL connection" \
    --secret-string "dedp_test"


awslocal secretsmanager create-secret \
    --name psql_password \
    --description "Password for PostgreSQL connection" \
    --secret-string "dedp_test"
```

⚠️ This definition step should be either done by your administrator or by the Infrastructure as Code (IaC) with
the goal to not reveal the secrets or at least, to limit the revelation scope.

3. Explain the [devices_json_converter.py](devices_json_converter.py)
* the job leverages AWS Secrets Manager to read the user and password secrets 
* these parameters are resolved at runtime and used as connection attributes

4. Generate the dataset:
```
cd dataset
docker-compose down --volumes; docker-compose up
```

5. Run the `devices_json_converter.py`. It should read PostgreSQL data and write it as JSON files.

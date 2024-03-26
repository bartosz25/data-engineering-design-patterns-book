import json

import urllib.parse
import requests


def lambda_handler(event, context):
    """
    Handles S3 events and triggers the DAG execution. The code simplifies several points that you should
    do differently on production:

    - Credentials used to connect to your Apache Airflow instance should be read from a Secret Manager service
      through an API
    - The Apache Airflow URLs could also be retrieved from a configuration store, so that the instance will be aware
      of any Airflow redeployment.


    """
    my_ip = '..........'
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }
    payload = {'event': json.dumps(event), 'trigger': {
        'function_name': context.function_name, 'function_version': context.function_version,
        'lambda_request_id': context.aws_request_id
    }, 'file_to_load': urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8'),
               'dag_run_id': f'External-{context.aws_request_id}'}
    trigger_response = requests.post(f'http://{my_ip}:8080/api/v1/dags/devices_loader/dagRuns', data=json.dumps({
        'conf': payload
    }), auth=('dedp', 'dedp'), headers=headers)
    print(trigger_response.text)
    if trigger_response.status_code != 200:
        raise Exception(f"Couldn't trigger the `devices_loader` DAG. {trigger_response} for {payload}")
    else:
        return True

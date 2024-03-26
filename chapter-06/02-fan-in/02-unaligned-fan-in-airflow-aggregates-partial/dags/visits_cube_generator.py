import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

with DAG('visits_cube_generator', max_active_runs=1,
         default_args={
             'depend_on_past': True,
             'start_date': pendulum.datetime(2024, 2, 1),
             'end_date': pendulum.datetime(2024, 2, 1),
             'retries': 3,
             'retry_delay': timedelta(minutes=1)
         },
         template_searchpath=[os.getcwd()],
         schedule_interval="@daily", catchup=True) as dag:

    clear_context = PostgresOperator(
        task_id='clear_context',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/clear_context.sql'
    )

    generate_cube = PostgresOperator(
        task_id='generate_cube',
        postgres_conn_id='docker_postgresql',
        database='dedp',
        sql='/sql/generate_visits_cube.sql',
        trigger_rule=TriggerRule.ALL_DONE
    )

    input_dir = '/tmp/dedp/ch06/02-fan-in/02-unaligned-fan-in-airflow-aggregates-partial/input'
    hours_to_load = [f"{hour:02d}" for hour in range(24)]
    for loaded_hour in hours_to_load:
        file_sensor = FileSensor(
            task_id=f'wait_for_{loaded_hour}',
            mode='reschedule',
            poke_interval=10,
            timeout=20,
            retries=2,
            filepath=input_dir +'/date={{ ds_nodash }}/hour=' + loaded_hour+'/dataset.csv'
        )
        visits_loader = PostgresOperator(
            task_id=f'load_hourly_visits_{loaded_hour}',
            postgres_conn_id='docker_postgresql',
            database='dedp',
            sql='/sql/load_visits.sql',
            params={
                'hour': loaded_hour
            }
        )

        clear_context >> file_sensor >> visits_loader >> generate_cube

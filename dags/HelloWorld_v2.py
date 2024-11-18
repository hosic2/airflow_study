from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# slack alert 함수를 생성한 py
from plugins import slack

@task
def print_hello():
    print("hello!")
    return "hello!"

@task
def print_goodbye():
    print("goodbye!")
    return "goodbye!"

with DAG(
    dag_id = 'HelloWorld_v2',
    start_date = datetime(2022,5,5),
    catchup=False,
    tags=['example'],
    schedule = '0 2 * * *',
    default_args = {
        # 'retries': 1,
        # 'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack.on_failure_callback, # DAG 실패 시 Alert
        'on_success_callback': slack.on_success_callback, # DAG 성공 시 Alert
    }
) as dag:

    # Assign the tasks to the DAG in order
    print_hello() >> print_goodbye()

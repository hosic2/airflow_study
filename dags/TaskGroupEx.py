from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from datetime import datetime

with DAG('task_group_example', start_date=datetime(2024, 11, 19), schedule_interval=None) as dag:
    start = DummyOperator(task_id='start')

    with TaskGroup('group1') as group1:
        task1 = DummyOperator(task_id='task1')
        task2 = DummyOperator(task_id='task2')
        task3 = DummyOperator(task_id='task3')
        task1 >> task2 >> task3

    with TaskGroup('group2') as group2:
        task4 = DummyOperator(task_id='task4')
        task5 = DummyOperator(task_id='task5')
        task4 >> task5

    end = DummyOperator(task_id='end')

    start >> group1 >> group2 >> end

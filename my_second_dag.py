from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import time

my_dag = DAG(
    dag_id='my_second_dag',
    description='A new DAG with two tasks',
    tags=['tutorial', 'datascientest'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(2),
    }
)


def print_task1():
    raise TypeError('This will not work')
    print('hello from task 1')


def print_task2():
    print('hello from task 2')


task1 = PythonOperator(
    task_id='second_dag_task1',
    python_callable=print_task1,
    dag=my_dag
)


task2 = PythonOperator(
    task_id='second_dag_task2',
    python_callable=print_task2,
    dag=my_dag
)
task1 >> task2
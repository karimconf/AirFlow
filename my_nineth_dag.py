from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import random

my_dag = DAG(
    dag_id='simple_xcom_dag',
    schedule_interval=None,
    start_date=days_ago(0)
)


def function_with_return(task_instance):
    task_instance.xcom_push(
        key="my_xcom_value",
        value={
            "hello": "world",
            "bonjour": "le monde"
        }
    )


my_task = PythonOperator(
    task_id='python_task',
    dag=my_dag,
    python_callable=function_with_return
)

def read_data_from_xcom(task_instance):
    print(
        task_instance.xcom_pull(
            key="my_xcom_value",
            task_ids=['python_task']
        )
    )


my_task2 = PythonOperator(
    task_id="read_Xcom_value",
    dag=my_dag,
    python_callable=read_data_from_xcom
)

my_task >> my_task2
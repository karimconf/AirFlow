from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
import time

my_dag = DAG(
    dag_id="documented_dag",
    doc_md="""# Documented DAG
This `DAG` is documented and the next line is a quote:

> Airflow is nice

This DAG has been made:

* by Paul
* with documentation
* with caution
    """,
    start_date=days_ago(0),
    schedule_interval=None
)


def sleep_1_sec():
    time.sleep(1)


task1 = PythonOperator(
    task_id="sleep1",
    python_callable=sleep_1_sec,
    doc_md="""# Task1

Task that is used to sleep for 1 sec""",
    dag=my_dag
)

task2 = PythonOperator(
    task_id="sleep2",
    python_callable=sleep_1_sec,
    doc="""Task 3

It has an ugly description.
    """,
    dag=my_dag
)
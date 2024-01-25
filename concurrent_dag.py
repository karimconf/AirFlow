from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import time


def wait_1_minute():
    time.sleep(60)


my_dag = DAG(
    dag_id="concurrent_dag",
    schedule_interval=None,
    start_date=days_ago(0),
    default_args={
        'pool': 'micro_pool'
    }

)

task1 = PythonOperator(
    task_id="wait1",
    python_callable=wait_1_minute,
    dag=my_dag
)

task2 = PythonOperator(
    task_id="wait2",
    python_callable=wait_1_minute,
    dag=my_dag
)

task3 = PythonOperator(
    task_id="wait3",
    python_callable=wait_1_minute,
    dag=my_dag
)
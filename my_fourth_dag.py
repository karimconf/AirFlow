from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import datetime

my_dag = DAG(
    dag_id='minutely_dag',
    description='My DAG that\'s triggered every minute',
    tags=['tutorial', 'datascientest'],
    schedule_interval='* * * * *',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    }
    
)


def print_date():
    print(datetime.datetime.now())


my_task = PythonOperator(
    task_id='print_date_task',
    dag=my_dag,
    python_callable=print_date
)
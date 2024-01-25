
import requests
import json
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 1, 1),
    'schedule_interval': '@every 1 minutes',
}

dag = DAG('weather_data_processing', default_args=default_args, catchup=False)

# Define a variable to store the list of cities
cities = ['Paris', 'London', 'Washington']

# Task 1: Retrieve weather data from OpenWeatherMap
def retrieve_weather_data():
    for city in cities:
        # Construct the URL to query OpenWeatherMap
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid=ebbbdf1d21237bdca7987a73379e21e6"

        # Send the GET request and extract the JSON data
        response = requests.get(url)
        data = response.json()

        # Format the date and time for the filename
        current_date = datetime.now().strftime("%Y-%m-%d %H:%M")

        # Generate the filename for the JSON file
        file_name = f"{current_date}.json"

        # Write the JSON data to the file
        with open(f"/app/raw_files/{file_name}", 'w') as json_file:
            json.dump(data, json_file)

# Task 2: Transform latest 20 weather observations into CSV (data.csv)
def transform_latest_20_data():
    transform_data_into_csv(n_files=20, filename='clean_data/data.csv')

latest_20_data_task = PythonOperator(
    task_id='transform_latest_20_data',
    python_callable=transform_latest_20_data,
    dag=dag
)

# Task 3: Transform all weather observations into CSV (fulldata.csv)
def transform_all_data():
    transform_data_into_csv(filename='clean_data/fulldata.csv')

all_data_task = PythonOperator(
    task_id='transform_all_data',
    python_callable=transform_all_data,
    dag=dag
)

# Set the DAG dependencies
retrieve_weather_data_task >> latest_20_data_task >> all_data_task


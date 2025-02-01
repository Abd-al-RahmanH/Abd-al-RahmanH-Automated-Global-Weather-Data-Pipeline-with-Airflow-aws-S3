from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
import json
import pandas as pd
import requests

# AWS Credentials (Hardcoded for now)
AWS_CREDENTIALS = {
    "key": "your acces key",
    "secret": "your secert access key",
}

# OpenWeatherMap API Key (Hardcoded for now)
API_KEY = "your openwhether api"

# S3 Bucket Name (Hardcoded for now)
S3_BUCKET = "newopenweatherdata"

# List of 30 cities
CITIES = [
    "New York", "London", "Paris", "Tokyo", "Mumbai", "Beijing", "Sydney",
    "Berlin", "Los Angeles", "Rio de Janeiro", "Moscow", "Dubai", "Toronto",
    "Singapore", "Mexico City", "Johannesburg", "Seoul", "Hong Kong", "Bangkok",
    "Rome", "Madrid", "Buenos Aires", "Jakarta", "Istanbul", "Cairo",
    "Lagos", "Lima", "Chicago", "Kuala Lumpur", "Melbourne"
]

# Function to convert Kelvin to Fahrenheit
def kelvin_to_fahrenheit(temp_in_kelvin):
    return (temp_in_kelvin - 273.15) * (9/5) + 32

# Function to extract weather data from OpenWeatherMap API
def extract_weather_data():
    weather_data = []
    
    for city in CITIES:
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            weather_data.append(data)
    
    return weather_data

# Function to transform and load data to S3
def transform_load_data(**kwargs):
    task_instance = kwargs['ti']
    data_list = task_instance.xcom_pull(task_ids="extract_weather_data")

    transformed_data = []

    for data in data_list:
        city = data["name"]
        weather_description = data["weather"][0]['description']
        temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
        feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
        min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
        max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
        pressure = data["main"]["pressure"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
        sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
        sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

        transformed_data.append({
            "City": city,
            "Description": weather_description,
            "Temperature (F)": temp_fahrenheit,
            "Feels Like (F)": feels_like_fahrenheit,
            "Minimum Temp (F)": min_temp_fahrenheit,
            "Maximum Temp (F)": max_temp_fahrenheit,
            "Pressure": pressure,
            "Humidity": humidity,
            "Wind Speed": wind_speed,
            "Time of Record": time_of_record,
            "Sunrise (Local Time)": sunrise_time,
            "Sunset (Local Time)": sunset_time
        })

    df_data = pd.DataFrame(transformed_data)

    # Generate unique filename
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    file_name = f"weather_data_{dt_string}.csv"

    # Save to S3
    df_data.to_csv(f"s3://{S3_BUCKET}/{file_name}", index=False, storage_options=AWS_CREDENTIALS)
    print(f"File uploaded successfully to s3://{S3_BUCKET}/{file_name}")

# Default args for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# DAG Definition
with DAG('weather_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:

    # Check API Availability
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=New York&APPID=' + API_KEY
    )

    # Extract Weather Data for Multiple Cities
    extract_weather_data_task = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data
    )

    # Transform and Load Data to S3
    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data,
        provide_context=True
    )

    # Task Dependencies
    is_weather_api_ready >> extract_weather_data_task >> transform_load_weather_data

from airflow import DAG # Import the DAG class from Airflow
from datetime import timedelta, datetime # Import the timedelta and datetime classes from Python's datetime module
from airflow.providers.http.sensors.http import HttpSensor # Import the HttpSensor class from Airflow's HTTP provider
import json # Import the json library
from airflow.providers.http.operators.http import SimpleHttpOperator # Import the SimpleHttpOperator class from Airflow's HTTP provider
from airflow.operators.python import PythonOperator # Import the PythonOperator class from Airflow
import pandas as pd # Import the Pandas library
import localKeys


def kelvin_to_fahrenheit(temp_in_kelvin): # Define a function to convert Kelvin to Fahrenheit
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32 # Convert the temperature from Kelvin to Fahrenheit
    return temp_in_fahrenheit # Return the converted temperature

def transform_load_data(task_instance): # Define a function to transform and load the weather data
    data = task_instance.xcom_pull(task_ids="extract_weather_data") # Get the weather data from the previous task
    city = data["name"] # Extract the city name from the weather data
    weather_description = data["weather"][0]['description'] # Extract the weather description from the weather data
    temp_farenheit = kelvin_to_fahrenheit (data[ "main"]["temp"]) # Convert the temperature to Fahrenheit
    feels_like_farenheit = kelvin_to_fahrenheit(data["main"]["feels_like"]) # Convert the feels-like temperature to Fahrenheit
    min_temp_farenheit = kelvin_to_fahrenheit(data[ "main"]["temp_min"]) # Convert the minimum temperature to Fahrenheit
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"]) # Convert the maximum temperature to Fahrenheit
    pressure = data["main"]["pressure"] # Extract the pressure from the weather data
    humidity = data["main"]["humidity"] # Extract the humidity from the weather data
    wind_speed = data["wind"]["speed" ] # Extract the wind speed from the weather data
    time_record = datetime.utcfromtimestamp(data['dt'] + data['timezone']) # Convert the time record to a Python datetime object
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone']) # Convert the sunrise time to a Python datetime object
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone']) # Convert the sunset time to a Python datetime object
    
    transformed_data = {"City": city, # Create a dictionary containing the transformed weather data
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)" :min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_record,
                        "Sunrise (Local Time)": sunrise_time,
                        "Sunset (Local Time)": sunset_time
                        }
   
    transformed_data_list = [transformed_data] # Create a list containing the transformed weather data
    df_data = pd.DataFrame(transformed_data_list) # Create a Pandas DataFrame from the transformed weather data
    aws_credientals = {
        "key": localKeys.API_KEY,
        "secret": localKeys.SECRET,
        "token": localKeys.TOKEN
        }
    
    now = datetime.now() # Get the current date and time
    dt_string = now.strftime("%d%m%Y%H%M%S") # Format the current date and time
    dt_string = 'current_weather_data_philadelphia_' + dt_string # The CSV file is named current_weather_data_philadelphia_
    df_data.to_csv(f"s3://weather-api-airflow/{dt_string}.csv", index=False, storage_options=aws_credientals)

# This section defines the default arguments for the DAG. These arguments will be applied to all tasks in the DAG, unless overridden by the task-specific arguments. 
default_args = { # The default arguments in this case specify that...
    'owner': 'airflow', # the owner of the DAG is airflow
    'depends_on_past': False, # the DAG does not depend on any previous DAGs
    'start_date': datetime(2023, 1, 8), # the start date of the DAG is January 8, 2023
    'email': [localKeys.EMAIL], # email notifications to be sent to justin.v.goncalves@outlook.com in the event of a failure
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2, # tasks should be retried 2 times 
    'retry_delay': timedelta(minutes=2) # with a delay of 2 minutes between retries
}



with DAG('weather_dag', # Define a new Airflow DAG named 'weather_dag'.
        default_args=default_args, # Set the default arguments for this DAG.
        schedule_interval = '@daily', # Schedule the DAG to run daily.
        catchup=False) as dag: # Specify that the DAG should not catch up on missed tasks.
    
    is_weather_api_ready = HttpSensor( # Create an HTTP sensor task named 'is_weather_api_ready'.
        task_id = 'is_weather_api_ready', # Assign the task ID 'is_weather_api_ready'.
        http_conn_id='weathermap_api', # Configure the HTTP sensor to use the 'weathermap_api' connection.
        endpoint= localKeys.ENDPOINT # Specify the API endpoint to check for weather data in Philadelphia.
        # This sensor will determine whether the API is available and responding successfully.
    )
        
    extract_weather_data = SimpleHttpOperator( # Create a Simple HTTP operator task.
        task_id = 'extract_weather_data', # Assign the task ID 'extract_weather_data'.
        http_conn_id = 'weathermap_api', # Configure the HTTP operator to use the 'weathermap_api' connection.
        endpoint= localKeys.ENDPOINT, # Specify the API endpoint to extract weather data for Philadelphia.
        method = 'GET', # Use the HTTP GET method to retrieve data from the API.
        response_filter=lambda r: json.loads(r.text), # Define a response filter to parse the JSON response.
        # Log_response=True # (Optional) Enable logging of the API response.
    )

    transform_load_weather_data = PythonOperator( # Create a Python operator task.
        task_id= 'transform_load_weather_data', # Assign the task ID 'transform_load_weather_data'.
        python_callable=transform_load_data # Specify the Python function to be executed for this task.
    )




    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data # Define task dependencies within the DAG.


# Databricks notebook source
# MAGIC %md
# MAGIC Sending a testing data to the event hub

# COMMAND ----------

from azure.eventhub import EventHubProducerClient , EventData
import json
import os
#Event Hub Connection String
EVENT_HUB_CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
EVENT_HUB_NAME = "streamingweathereventhub"
#intioalize the event hub producer
producer=EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME)
#function to send events to Event Hub
def send_event(event):
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)   
#sample Json Event
event = {
    "event_id": 1111,
    "event_name":"Test Event",
}
#send the event
send_event(event)
#close the producer
producer.close()

# COMMAND ----------

from azure.eventhub import EventHubProducerClient , EventData
import json
#Event Hub Connection String
EVENT_HUB_CONNECTION_STRING= dbutils.secrets.get(scope = "key-vault-scope", key = "eventhub-connection-string")
EVENT_HUB_NAME = "streamingweathereventhub"
#intioalize the event hub producer
producer=EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME)
#function to send events to Event Hub
def send_event(event):
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)   
#sample Json Event
event = {
    "event_id": 2222,
    "event_name":"Key Vault",
}
#send the event
send_event(event)
#close the producer
producer.close()

# COMMAND ----------

import requests 
import json

#getting secrete value frim key vault
weatherapikey = dbutils.secrets.get(scope = "key-vault-scope", key = "weatherapikeyy")
location="Delhi"

base_url=" http://api.weatherapi.com/v1"

current_weather_url =f"{base_url}/current.json"

params={
    'key':weatherapikey,
    'q':location
}
response=requests.get(current_weather_url,params=params)

if response.status_code==200:
    current_weather=response.json()
    print("Current Weather:")
    print(json.dumps(current_weather,indent=3))
   
else:
    print(f"Error:{response.status_code},{response.text}")

# COMMAND ----------

import requests 
import json

#Funtion to handle the api response 
def handle_response(response):
    if response.status_code==200:
        return response.json()
    else:
        print(f"Error:{response.status_code},{response.text}")

#function to get current weather and air quality response
def get_current_weather(base_url,api_key,location):
    current_weather_url =f"{base_url}/current.json"
    params={
        'key':api_key,
        'q':location,
        'aqi':'yes'
    }
    response=requests.get(current_weather_url,params=params)
    return handle_response(response)

#Funtion to get Forecast data
def get_forecast_weather(base_url,api_key,location,days):
    forecast_weather_url =f"{base_url}/forecast.json"
    params={
        'key':api_key,
        'q':location,
        'days':days
    }
    response=requests.get(forecast_weather_url,params=params)
    return handle_response(response)

# funtion to get the Alerts
def get_alerts_url(base_url,api_key,location):
    alerts_url =f"{base_url}/alerts.json"
    params={
        'key':api_key,
        'q':location,
        'alerts':'yes',
    }
    response=requests.get(alerts_url,params=params)
    return handle_response(response)


# Flatten and merge the data
def flatten_data(current_weather, forecast_weather, alerts):
    location_data = current_weather.get("location", {})
    current = current_weather.get("current", {})
    condition = current.get("condition", {})
    air_quality = current.get("air_quality", {})
    forecast = forecast_weather.get("forecast", {}).get("forecastday", [])
    alert_list = alerts.get("alerts", {}).get("alert", [])

    flattened_data = {
        'name': location_data.get('name'),
        'region': location_data.get('region'),
        'country': location_data.get('country'),
        'lat': location_data.get('lat'),
        'lon': location_data.get('lon'),
        'localtime': location_data.get('localtime'),
        'temp_c': current.get('temp_c'),
        'is_day': current.get('is_day'),
        'condition_text': condition.get('text'),
        'condition_icon': condition.get('icon'),
        'wind_kph': current.get('wind_kph'),
        'wind_degree': current.get('wind_degree'),
        'wind_dir': current.get('wind_dir'),
        'pressure_in': current.get('pressure_in'),
        'precip_in': current.get('precip_in'),
        'humidity': current.get('humidity'),
        'cloud': current.get('cloud'),
        'feelslike_c': current.get('feelslike_c'),
        'uv': current.get('uv'),
        'air_quality': {
            'co': air_quality.get('co'),
            'no2': air_quality.get('no2'),
            'o3': air_quality.get('o3'),
            'so2': air_quality.get('so2'),
            'pm2_5': air_quality.get('pm2_5'),
            'pm10': air_quality.get('pm10'),
            'us-epa-index': air_quality.get('us-epa-index'),
            'gb-defra-index': air_quality.get('gb-defra-index')
        },
        'alerts': [
            {
                'headline': alert.get('headline'),
                'severity': alert.get('severity'),
                'description': alert.get('desc'),
                'instruction': alert.get('instruction')
            }
            for alert in alert_list
        ],
        'forecast': [
            {
                'date': day.get('date'),
                'maxtemp_c': day.get('day', {}).get('maxtemp_c'),
                'mintemp_c': day.get('day', {}).get('mintemp_c'),
                'condition': day.get('day', {}).get('condition', {}).get('text')
            }
            for day in forecast
        ]
    }
    return flattened_data

#main funtion 

def fetch_weather_data():
    base_url=" http://api.weatherapi.com/v1"
    api_key = dbutils.secrets.get(scope = "key-vault-scope", key = "weatherapikeyy")
    location="Ghaziabad"
    days=3
    current_weather = get_current_weather(base_url,api_key,location)
    forecast_weather = get_forecast_weather(base_url,api_key,location,days)
    alerts = get_alerts_url(base_url,api_key,location)
    merged_data = flatten_data(current_weather, forecast_weather, alerts)
    print("Weather Data:", json.dumps(merged_data, indent=3))

# calling the main program
fetch_weather_data()



# COMMAND ----------

# MAGIC %md
# MAGIC complete code getting weather api

# COMMAND ----------

import requests 
import json
from azure.eventhub import EventHubProducerClient, EventData

#Event Hub Connection String
EVENT_HUB_CONNECTION_STRING= dbutils.secrets.get(scope = "key-vault-scope", key = "eventhub-connection-string")
EVENT_HUB_NAME = "streamingweathereventhub"

#intioalize the event hub producer
producer=EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME)

#function to send events to Event Hub
def send_event(event):
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)   

#Funtion to handle the api response 
def handle_response(response):
    if response.status_code==200:
        return response.json()
    else:
        print(f"Error:{response.status_code},{response.text}")

#function to get current weather and air quality response
def get_current_weather(base_url,api_key,location):
    current_weather_url =f"{base_url}/current.json"
    params={
        'key':api_key,
        'q':location,
        'aqi':'yes'
    }
    response=requests.get(current_weather_url,params=params)
    return handle_response(response)

#Funtion to get Forecast data
def get_forecast_weather(base_url,api_key,location,days):
    forecast_weather_url =f"{base_url}/forecast.json"
    params={
        'key':api_key,
        'q':location,
        'days':days
    }
    response=requests.get(forecast_weather_url,params=params)
    return handle_response(response)

# funtion to get the Alerts
def get_alerts_url(base_url,api_key,location):
    alerts_url =f"{base_url}/alerts.json"
    params={
        'key':api_key,
        'q':location,
        'alerts':'yes',
    }
    response=requests.get(alerts_url,params=params)
    return handle_response(response)


# Flatten and merge the data
def flatten_data(current_weather, forecast_weather, alerts):
    location_data = current_weather.get("location", {})
    current = current_weather.get("current", {})
    condition = current.get("condition", {})
    air_quality = current.get("air_quality", {})
    forecast = forecast_weather.get("forecast", {}).get("forecastday", [])
    alert_list = alerts.get("alerts", {}).get("alert", [])

    flattened_data = {
        'name': location_data.get('name'),
        'region': location_data.get('region'),
        'country': location_data.get('country'),
        'lat': location_data.get('lat'),
        'lon': location_data.get('lon'),
        'localtime': location_data.get('localtime'),
        'temp_c': current.get('temp_c'),
        'is_day': current.get('is_day'),
        'condition_text': condition.get('text'),
        'condition_icon': condition.get('icon'),
        'wind_kph': current.get('wind_kph'),
        'wind_degree': current.get('wind_degree'),
        'wind_dir': current.get('wind_dir'),
        'pressure_in': current.get('pressure_in'),
        'precip_in': current.get('precip_in'),
        'humidity': current.get('humidity'),
        'cloud': current.get('cloud'),
        'feelslike_c': current.get('feelslike_c'),
        'uv': current.get('uv'),
        'air_quality': {
            'co': air_quality.get('co'),
            'no2': air_quality.get('no2'),
            'o3': air_quality.get('o3'),
            'so2': air_quality.get('so2'),
            'pm2_5': air_quality.get('pm2_5'),
            'pm10': air_quality.get('pm10'),
            'us-epa-index': air_quality.get('us-epa-index'),
            'gb-defra-index': air_quality.get('gb-defra-index')
        },
        'alerts': [
            {
                'headline': alert.get('headline'),
                'severity': alert.get('severity'),
                'description': alert.get('desc'),
                'instruction': alert.get('instruction')
            }
            for alert in alert_list
        ],
        'forecast': [
            {
                'date': day.get('date'),
                'maxtemp_c': day.get('day', {}).get('maxtemp_c'),
                'mintemp_c': day.get('day', {}).get('mintemp_c'),
                'condition': day.get('day', {}).get('condition', {}).get('text')
            }
            for day in forecast
        ]
    }
    return flattened_data

#main funtion 

def fetch_weather_data():
    base_url=" http://api.weatherapi.com/v1"
    api_key = dbutils.secrets.get(scope = "key-vault-scope", key = "weatherapikeyy")
    location="Ghaziabad"
    days=3
    current_weather = get_current_weather(base_url,api_key,location)
    forecast_weather = get_forecast_weather(base_url,api_key,location,days)
    alerts = get_alerts_url(base_url,api_key,location)
    merged_data = flatten_data(current_weather, forecast_weather, alerts)
    send_event(merged_data)
    print("Weather Data:", json.dumps(merged_data, indent=3))

# calling the main program
fetch_weather_data()



# COMMAND ----------

# MAGIC %md
# MAGIC sending weather data in streaming fashion

# COMMAND ----------

import requests 
import json
from azure.eventhub import EventHubProducerClient, EventData

#Event Hub Connection String
EVENT_HUB_CONNECTION_STRING= dbutils.secrets.get(scope = "key-vault-scope", key = "eventhub-connection-string")
EVENT_HUB_NAME = "streamingweathereventhub"

#intioalize the event hub producer
producer=EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME)

#function to send events to Event Hub
def send_event(event):
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)   

#Funtion to handle the api response 
def handle_response(response):
    if response.status_code==200:
        return response.json()
    else:
        print(f"Error:{response.status_code},{response.text}")

#function to get current weather and air quality response
def get_current_weather(base_url,api_key,location):
    current_weather_url =f"{base_url}/current.json"
    params={
        'key':api_key,
        'q':location,
        'aqi':'yes'
    }
    response=requests.get(current_weather_url,params=params)
    return handle_response(response)

#Funtion to get Forecast data
def get_forecast_weather(base_url,api_key,location,days):
    forecast_weather_url =f"{base_url}/forecast.json"
    params={
        'key':api_key,
        'q':location,
        'days':days
    }
    response=requests.get(forecast_weather_url,params=params)
    return handle_response(response)

# funtion to get the Alerts
def get_alerts_url(base_url,api_key,location):
    alerts_url =f"{base_url}/alerts.json"
    params={
        'key':api_key,
        'q':location,
        'alerts':'yes',
    }
    response=requests.get(alerts_url,params=params)
    return handle_response(response)


# Flatten and merge the data
def flatten_data(current_weather, forecast_weather, alerts):
    location_data = current_weather.get("location", {})
    current = current_weather.get("current", {})
    condition = current.get("condition", {})
    air_quality = current.get("air_quality", {})
    forecast = forecast_weather.get("forecast", {}).get("forecastday", [])
    alert_list = alerts.get("alerts", {}).get("alert", [])

    flattened_data = {
        'name': location_data.get('name'),
        'region': location_data.get('region'),
        'country': location_data.get('country'),
        'lat': location_data.get('lat'),
        'lon': location_data.get('lon'),
        'localtime': location_data.get('localtime'),
        'temp_c': current.get('temp_c'),
        'is_day': current.get('is_day'),
        'condition_text': condition.get('text'),
        'condition_icon': condition.get('icon'),
        'wind_kph': current.get('wind_kph'),
        'wind_degree': current.get('wind_degree'),
        'wind_dir': current.get('wind_dir'),
        'pressure_in': current.get('pressure_in'),
        'precip_in': current.get('precip_in'),
        'humidity': current.get('humidity'),
        'cloud': current.get('cloud'),
        'feelslike_c': current.get('feelslike_c'),
        'uv': current.get('uv'),
        'air_quality': {
            'co': air_quality.get('co'),
            'no2': air_quality.get('no2'),
            'o3': air_quality.get('o3'),
            'so2': air_quality.get('so2'),
            'pm2_5': air_quality.get('pm2_5'),
            'pm10': air_quality.get('pm10'),
            'us-epa-index': air_quality.get('us-epa-index'),
            'gb-defra-index': air_quality.get('gb-defra-index')
        },
        'alerts': [
            {
                'headline': alert.get('headline'),
                'severity': alert.get('severity'),
                'description': alert.get('desc'),
                'instruction': alert.get('instruction')
            }
            for alert in alert_list
        ],
        'forecast': [
            {
                'date': day.get('date'),
                'maxtemp_c': day.get('day', {}).get('maxtemp_c'),
                'mintemp_c': day.get('day', {}).get('mintemp_c'),
                'condition': day.get('day', {}).get('condition', {}).get('text')
            }
            for day in forecast
        ]
    }
    return flattened_data



def fetch_weather_data():
    base_url=" http://api.weatherapi.com/v1"
    api_key = dbutils.secrets.get(scope = "key-vault-scope", key = "weatherapikeyy")
    location="Ghaziabad"
    days=3
    current_weather = get_current_weather(base_url,api_key,location)
    forecast_weather = get_forecast_weather(base_url,api_key,location,days)
    alerts = get_alerts_url(base_url,api_key,location)
    merged_data = flatten_data(current_weather, forecast_weather, alerts)
    send_event(merged_data)
   

#main funtion 
def process_batch(batch_df,batch_id):
    try:
        weather_data=fetch_weather_data()
        send_event(weather_data)
    except Exception as e:
        print(f"Error sending event in batch{batch_id}:{str(e)}")
        raise e

streaming_df=spark.readStream.format("rate").option("rows per second","1").load()
query=streaming_df.writeStream.foreachBatch(process_batch).start()
query.awaitTermination()
producer.close()

# COMMAND ----------

# MAGIC %md
# MAGIC for every 30 sec

# COMMAND ----------

import requests 
import json
from azure.eventhub import EventHubProducerClient, EventData
from datetime import datetime, timedelta

#Event Hub Connection String
EVENT_HUB_CONNECTION_STRING= dbutils.secrets.get(scope = "key-vault-scope", key = "eventhub-connection-string")
EVENT_HUB_NAME = "streamingweathereventhub"

#intioalize the event hub producer
producer=EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STRING, eventhub_name=EVENT_HUB_NAME)

#function to send events to Event Hub
def send_event(event):
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)   

#Funtion to handle the api response 
def handle_response(response):
    if response.status_code==200:
        return response.json()
    else:
        print(f"Error:{response.status_code},{response.text}")

#function to get current weather and air quality response
def get_current_weather(base_url,api_key,location):
    current_weather_url =f"{base_url}/current.json"
    params={
        'key':api_key,
        'q':location,
        'aqi':'yes'
    }
    response=requests.get(current_weather_url,params=params)
    return handle_response(response)

#Funtion to get Forecast data
def get_forecast_weather(base_url,api_key,location,days):
    forecast_weather_url =f"{base_url}/forecast.json"
    params={
        'key':api_key,
        'q':location,
        'days':days
    }
    response=requests.get(forecast_weather_url,params=params)
    return handle_response(response)

# funtion to get the Alerts
def get_alerts_url(base_url,api_key,location):
    alerts_url =f"{base_url}/alerts.json"
    params={
        'key':api_key,
        'q':location,
        'alerts':'yes',
    }
    response=requests.get(alerts_url,params=params)
    return handle_response(response)


# Flatten and merge the data
def flatten_data(current_weather, forecast_weather, alerts):
    location_data = current_weather.get("location", {})
    current = current_weather.get("current", {})
    condition = current.get("condition", {})
    air_quality = current.get("air_quality", {})
    forecast = forecast_weather.get("forecast", {}).get("forecastday", [])
    alert_list = alerts.get("alerts", {}).get("alert", [])

    flattened_data = {
        'name': location_data.get('name'),
        'region': location_data.get('region'),
        'country': location_data.get('country'),
        'lat': location_data.get('lat'),
        'lon': location_data.get('lon'),
        'localtime': location_data.get('localtime'),
        'temp_c': current.get('temp_c'),
        'is_day': current.get('is_day'),
        'condition_text': condition.get('text'),
        'condition_icon': condition.get('icon'),
        'wind_kph': current.get('wind_kph'),
        'wind_degree': current.get('wind_degree'),
        'wind_dir': current.get('wind_dir'),
        'pressure_in': current.get('pressure_in'),
        'precip_in': current.get('precip_in'),
        'humidity': current.get('humidity'),
        'cloud': current.get('cloud'),
        'feelslike_c': current.get('feelslike_c'),
        'uv': current.get('uv'),
        'air_quality': {
            'co': air_quality.get('co'),
            'no2': air_quality.get('no2'),
            'o3': air_quality.get('o3'),
            'so2': air_quality.get('so2'),
            'pm2_5': air_quality.get('pm2_5'),
            'pm10': air_quality.get('pm10'),
            'us-epa-index': air_quality.get('us-epa-index'),
            'gb-defra-index': air_quality.get('gb-defra-index')
        },
        'alerts': [
            {
                'headline': alert.get('headline'),
                'severity': alert.get('severity'),
                'description': alert.get('desc'),
                'instruction': alert.get('instruction')
            }
            for alert in alert_list
        ],
        'forecast': [
            {
                'date': day.get('date'),
                'maxtemp_c': day.get('day', {}).get('maxtemp_c'),
                'mintemp_c': day.get('day', {}).get('mintemp_c'),
                'condition': day.get('day', {}).get('condition', {}).get('text')
            }
            for day in forecast
        ]
    }
    return flattened_data



def fetch_weather_data():
    base_url="http://api.weatherapi.com/v1"
    api_key = dbutils.secrets.get(scope = "key-vault-scope", key = "weatherapikeyy")
    location="Ghaziabad"
    days=3
    current_weather = get_current_weather(base_url,api_key,location)
    forecast_weather = get_forecast_weather(base_url,api_key,location,days)
    alerts = get_alerts_url(base_url,api_key,location)
    merged_data = flatten_data(current_weather, forecast_weather, alerts)
    return merged_data
   
last_sent_time=datetime.now()-timedelta(seconds=30)
#main funtion 
def process_batch(batch_df,batch_id):
    global last_sent_time   
    try:
        current_time=datetime.now()
        if((current_time-last_sent_time).total_seconds()>=30):
            weather_data=fetch_weather_data()
            
            send_event(weather_data)
            last_sent_time=current_time
            print(f"Event sent at {current_time}")
    except Exception as e:
        print(f"Error sending event in batch{batch_id}:{str(e)}")
        raise e

streaming_df=spark.readStream.format("rate").option("rowsPerSecond","1").load()
query=streaming_df.writeStream.foreachBatch(process_batch).start()
query.awaitTermination()
producer.close()
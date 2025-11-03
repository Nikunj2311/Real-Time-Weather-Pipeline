import os
import json
import logging
import requests
from datetime import datetime

from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

logging.basicConfig(level=logging.INFO)

# Environment variables
EVENT_HUB_NAMESPACE = os.getenv("EVENT_HUB_NAMESPACE", "streaming-weather-eh.servicebus.windows.net")
EVENT_HUB_NAME = os.getenv("EVENT_HUB_NAME", "streamingweathereventhub")
KEYVAULT_NAME = os.getenv("KEYVAULT_NAME", "streaming-weather-kv")
API_KEY_SECRET_NAME = os.getenv("API_KEY_SECRET_NAME", "weatherapikey")
CITY = os.getenv("CITY", "Chennai")
FORECAST_DAYS = int(os.getenv("FORECAST_DAYS", "3"))

# Managed Identity Credentials
credential = DefaultAzureCredential()

# Event Hub producer
producer = EventHubProducerClient(
    fully_qualified_namespace=EVENT_HUB_NAMESPACE,
    eventhub_name=EVENT_HUB_NAME,
    credential=credential
)

# Key Vault client
vault_url = f"https://{KEYVAULT_NAME}.vault.azure.net/"
secret_client = SecretClient(vault_url=vault_url, credential=credential)
WEATHER_API_KEY = secret_client.get_secret(API_KEY_SECRET_NAME).value

BASE_URL = "http://api.weatherapi.com/v1/"

def send_event(event):
    """Send a single JSON event to Event Hub"""
    event_data_batch = producer.create_batch()
    event_data_batch.add(EventData(json.dumps(event)))
    producer.send_batch(event_data_batch)
    logging.info(f"[{datetime.now()}] Sent event to Event Hub")

def handle_response(response):
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Error: {response.status_code}, {response.text}")
        return {}

def get_current_weather():
    url = f"{BASE_URL}current.json"
    params = {"key": WEATHER_API_KEY, "q": CITY, "aqi": "yes"}
    response = requests.get(url, params=params)
    return handle_response(response)

def get_forecast_weather():
    url = f"{BASE_URL}forecast.json"
    params = {"key": WEATHER_API_KEY, "q": CITY, "days": FORECAST_DAYS}
    response = requests.get(url, params=params)
    return handle_response(response)

def get_alerts():
    url = f"{BASE_URL}alerts.json"
    params = {"key": WEATHER_API_KEY, "q": CITY, "alerts": "yes"}
    response = requests.get(url, params=params)
    return handle_response(response)

def flatten_data(current_weather, forecast_weather, alerts):
    location_data = current_weather.get("location", {})
    current = current_weather.get("current", {})
    condition = current.get("condition", {})
    air_quality = current.get("air_quality", {})
    forecast = forecast_weather.get("forecast", {}).get("forecastday", [])
    alert_list = alerts.get("alerts", {}).get("alert", [])

    flattened_data = {
        "name": location_data.get("name"),
        "region": location_data.get("region"),
        "country": location_data.get("country"),
        "lat": location_data.get("lat"),
        "lon": location_data.get("lon"),
        "localtime": location_data.get("localtime"),
        "temp_c": current.get("temp_c"),
        "is_day": current.get("is_day"),
        "condition_text": condition.get("text"),
        "condition_icon": condition.get("icon"),
        "wind_kph": current.get("wind_kph"),
        "wind_degree": current.get("wind_degree"),
        "wind_dir": current.get("wind_dir"),
        "pressure_in": current.get("pressure_in"),
        "precip_in": current.get("precip_in"),
        "humidity": current.get("humidity"),
        "cloud": current.get("cloud"),
        "feelslike_c": current.get("feelslike_c"),
        "uv": current.get("uv"),
        "air_quality": {
            "co": air_quality.get("co"),
            "no2": air_quality.get("no2"),
            "o3": air_quality.get("o3"),
            "so2": air_quality.get("so2"),
            "pm2_5": air_quality.get("pm2_5"),
            "pm10": air_quality.get("pm10"),
            "us-epa-index": air_quality.get("us-epa-index"),
            "gb-defra-index": air_quality.get("gb-defra-index"),
        },
        "alerts": [
            {
                "headline": alert.get("headline"),
                "severity": alert.get("severity"),
                "description": alert.get("desc"),
                "instruction": alert.get("instruction"),
            }
            for alert in alert_list
        ],
        "forecast": [
            {
                "date": day.get("date"),
                "maxtemp_c": day.get("day", {}).get("maxtemp_c"),
                "mintemp_c": day.get("day", {}).get("mintemp_c"),
                "condition": day.get("day", {}).get("condition", {}).get("text"),
            }
            for day in forecast
        ],
    }
    return flattened_data

def main():
    logging.info(f"[{datetime.now()}] Starting weather fetch job")
    current = get_current_weather()
    forecast = get_forecast_weather()
    alerts = get_alerts()
    merged_data = flatten_data(current, forecast, alerts)
    send_event(merged_data)
    logging.info(f"[{datetime.now()}] Weather job completed")

if __name__ == "__main__":
    main()

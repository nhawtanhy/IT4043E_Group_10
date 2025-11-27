from kafka import KafkaProducer
import requests
import json
import os
from datetime import datetime
import time

API_KEY = "4edad24e446460079dd56dd6264f4c7d"
CITY_LIST = ["Hanoi"]
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
KAFKA_TOPIC = 'weather-data'  # Kafka topic

def fetch_weather_data(city):
    """Fetch weather data for a city from OpenWeather API."""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {city}")
        return None

def send_to_kafka(city, data):
    """Send the weather data to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data to JSON
    )
    producer.send(KAFKA_TOPIC, data)
    producer.flush()
    print(f"Sent weather data for {city} to Kafka")

def main():
    for city in CITY_LIST:
        print(f"[{datetime.now()}] Collecting weather data for {city}...")
        data = fetch_weather_data(city)
        if data:
            send_to_kafka(city, data)
        time.sleep(10)  # Avoid hitting rate limits of the API

if __name__ == "__main__":
    main()

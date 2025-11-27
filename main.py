import requests
import json
import os
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY_LIST = ["Hanoi"]
KAFKA_TOPIC = 'weather_data'
KAFKA_SERVER = 'kafka:29092'

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("[INFO] Kafka producer connected.")
            return producer
        except NoBrokersAvailable:
            print("[WARN] Kafka not ready. Retrying in 5 seconds...")
            time.sleep(5)

producer = create_producer()

def fetch_weather_data(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"[ERROR] Failed to fetch data for {city}. Status: {response.status_code}")
            return None
    except Exception as e:
        print(f"[ERROR] {e}")
        return None

def send_to_kafka(city, data):
    message = {
        'city': city,
        'temperature': data['main']['temp'],
        'humidity': data['main']['humidity'],
        'weather': data['weather'][0]['description'],
        'timestamp': datetime.now().isoformat()
    }
    producer.send(KAFKA_TOPIC, value=message)
    print(f"[INFO] Sent data for {city} to Kafka")

def main():
    while True:
        for city in CITY_LIST:
            print(f"[INFO] Fetching weather data for {city} ...")
            data = fetch_weather_data(city)
            if data:
                send_to_kafka(city, data)
            time.sleep(10)

if __name__ == "__main__":
    main()

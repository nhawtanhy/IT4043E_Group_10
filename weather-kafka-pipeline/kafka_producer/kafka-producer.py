import os
import requests
import json
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "weather_raw"
API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]

try:
    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "socket.timeout.ms": 5000
    })
    kafka_available = True
except Exception as e:
    print(f"⚠️ Kafka not available: {e}")
    kafka_available = False

def fetch_weather_current(city):
    """Fetches CURRENT weather data for Kafka."""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            
            # Change type
            now = datetime.utcnow()
            now = now + timedelta(hours=7)
            
            return {
                "city": city,
                "timestamp": now.isoformat(),
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather": data["weather"][0]["description"],
                "raw": data 
            }
        print(f"[WARN] API {res.status_code} for {city}")
    except Exception as e:
        print(f"[ERROR] API Request failed: {e}")
    return None

def send_to_kafka(record):
    if not kafka_available or not record:
        return
    try:
        producer.produce(
            TOPIC,
            key=record['city'].encode("utf-8"),
            value=json.dumps(record).encode("utf-8")
        )
        producer.poll(0)
    except Exception as e:
        print(f"   ❌ Kafka Error: {e}")

def flatten_record(record):
    raw = record['raw']
    return {
        'city': record['city'],
        'timestamp': record['timestamp'],
        'description': record['weather'],
        'temp': raw['main']['temp'],
        'pressure': raw['main']['pressure'],
        'humidity': raw['main']['humidity'],
        'wind_speed': raw['wind'].get('speed', 0),
        'wind_deg': raw['wind'].get('deg', 0),
        'wind_gust': raw['wind'].get('gust', 0),
        'cloudiness': raw['clouds'].get('all', 0)
    }

def main():
    while True:
        for city in CITY_LIST:
            # 1. Fetch Current
            raw_data = fetch_weather_current(city)
            if raw_data:
                flat_data = flatten_record(raw_data)
                send_to_kafka(flat_data)
        if kafka_available:
            # Debug: After the first run there are 5 items in producer.
            producer.flush()
        time.sleep(60)

if __name__ == "__main__":
    main()
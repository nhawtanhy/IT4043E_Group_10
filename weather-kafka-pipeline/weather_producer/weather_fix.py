import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather-raw")
API_KEY = os.getenv("OPENWEATHER_API_KEY")

CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang"]
POLL_INTERVAL = 60

producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKER,
    }
)


def fetch_weather(city):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric",
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()

        event_time = datetime.now(timezone.utc).isoformat()

        return {
            "city": city,
            "event_time": event_time,
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
        }
    except Exception:
        return None


def send_to_kafka(record):
    producer.produce(
        topic=TOPIC,
        key=record["city"].encode(),
        value=json.dumps(record).encode(),
    )
    producer.poll(0)


def main():
    print("Weather producer running")

    while True:
        for city in CITY_LIST:
            record = fetch_weather(city)
            if record:
                print(record)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()

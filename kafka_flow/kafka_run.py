import json
import os
import time
from datetime import datetime

import requests
from kafka import KafkaProducer

API_KEY = os.getenv("OPENWEATHER_API_KEY", "YOUR_API_KEY")

CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]

KAFKA_BROKER = "localhost:9092"
TOPIC = "weather_raw"

# -------------------------------
# Kafka Producer setup
# -------------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)


def fetch_weather(city):
    """Gọi API thời tiết và trả về dict."""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"

    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            print(f"[WARN] {city} API failed: {res.text}")
            return None

        data = res.json()

        return {
            "city": city,
            "timestamp": datetime.utcnow().isoformat(),
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather": data["weather"][0]["description"],
            "raw_json": data,  # gửi full cho downstream
        }

    except Exception as e:
        print(f"[ERROR] Request failed for {city}: {e}")
        return None


def main():
    print("=== Weather → Kafka Producer started ===")

    while True:
        for city in CITY_LIST:
            record = fetch_weather(city)
            if record:
                producer.send(TOPIC, record)
                print(f"[PUSHED] {city}: {record['temperature']}°C")

        producer.flush()
        print("Sleeping 60s...\n")
        time.sleep(60)  # mỗi 1 phút gửi 1 lần


if __name__ == "__main__":
    main()

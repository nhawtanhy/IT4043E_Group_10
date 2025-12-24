import json
import os
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

# ================= CONFIG =================
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "weather-raw")
API_KEY = os.getenv("OPENWEATHER_API_KEY")

CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "60"))  # seconds

# ================= KAFKA =================
producer = Producer(
    {
        "bootstrap.servers": KAFKA_BROKER,
        "linger.ms": 50,
        "acks": "all",
    }
)


# ================= WEATHER =================
def fetch_weather(city: str) -> dict | None:
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric",
    }

    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            print(f"[WARN] API {r.status_code} for {city}")
            return None

        data = r.json()

        # Align to hour (important for windowing later)
        event_time = (
            datetime.now(timezone.utc)
            .replace(minute=0, second=0, microsecond=0)
            .isoformat()
        )

        return {
            "city": city,
            "event_time": event_time,
            "temp": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "pressure": data["main"]["pressure"],
            "wind_speed": data["wind"].get("speed", 0.0),
            "weather": data["weather"][0]["description"],
        }

    except Exception as e:
        print(f"[ERROR] API error for {city}: {e}")
        return None


# ================= PRODUCE =================
def send_to_kafka(record: dict):
    producer.produce(
        topic=TOPIC,
        key=record["city"].encode(),
        value=json.dumps(record).encode(),
    )
    producer.poll(0)


# ================= MAIN LOOP =================
def main():
    print(f"🚀 Weather producer started → topic={TOPIC}")

    while True:
        for city in CITY_LIST:
            record = fetch_weather(city)
            if record:
                send_to_kafka(record)

        producer.flush()
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()

import json
import os
import time
from datetime import datetime

import requests
from confluent_kafka import Producer

API_KEY = os.getenv("OPENWEATHER_API_KEY")
SEND_INTERVAL = int(os.getenv("SEND_INTERVAL", 60))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "weather_raw"

print("=========== ENV ===========")
print("API KEY:", API_KEY)
print("KAFKA:", KAFKA_BROKER)
print("SEND_INTERVAL:", SEND_INTERVAL)
print("===========================\n")

if not API_KEY:
    raise RuntimeError("Missing OPENWEATHER_API_KEY!")


# ================================
# Kafka Producer (Confluent client)
# ================================
p = Producer({"bootstrap.servers": KAFKA_BROKER})


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"📤 Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


def fetch_weather(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"

    try:
        res = requests.get(url, timeout=10)
        if res.status_code != 200:
            print(f"[WARN] API failed for {city}: {res.text}")
            return None

        data = res.json()

        return {
            "city": city,
            "timestamp": datetime.utcnow().isoformat(),
            "temperature": data["main"]["temp"],
            "humidity": data["main"]["humidity"],
            "weather": data["weather"][0]["description"],
        }

    except Exception as e:
        print(f"[ERROR] API error for {city}: {e}")
        return None


def main():
    print("🚀 Weather Producer Started!")

    while True:
        for city in ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]:
            record = fetch_weather(city)
            if record:
                p.produce(
                    TOPIC,
                    key=city.encode("utf-8"),
                    value=json.dumps(record).encode("utf-8"),
                    callback=delivery_report,
                )
                p.poll(0)

        p.flush()
        print(f"Sleeping {SEND_INTERVAL}s...\n")
        time.sleep(SEND_INTERVAL)


if __name__ == "__main__":
    main()

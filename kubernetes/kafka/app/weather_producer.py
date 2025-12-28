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

CITY_LIST = [
    "An Giang",
    "Bac Ninh",
    "Ca Mau",
    "Cao Bang",
    "Can Tho",
    "Da Nang",
    "Buon Ma Thuot",  # approximate Đắk lắk
    "Dien Bien Phu",  # approximate Điện Biên
    "Can Gio",  # approximate Đồng Nai
    "Cao Lanh",  # approximate Đồng Tháp
    "Gia Lai",
    "Hanoi",
    "Hà Tĩnh",
    "Haiphong",
    "Ho Chi Minh City",
    "Hung Yen",
    "Khánh Hòa",
    "Lai Chau",
    "Da Lat",  # approximate Lâm Đồng
    "Lang Son",
    "Lao Cai",
    "Vinh",  # approximate Nghệ An
    "Ninh Binh",
    "Phu Tho",
    "Quang Ngai",
    "Cam Pha Mines",  # approximate Quảng Ninh
    "Quảng Trị",
    "Son La",
    "Tay Ninh",
    "Thai Nguyen",
    "Thanh Hoa",
    "Hue",
    "Tuyen Quang",
    "Vinh Long",
]
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

        # Align event_time to hour (UTC)
        event_time = (
            datetime.now(timezone.utc)
            .replace(minute=0, second=0, microsecond=0)
            .isoformat()
        )

        weather = data.get("weather", [{}])[0]
        main = data.get("main", {})
        wind = data.get("wind", {})
        clouds = data.get("clouds", {})
        sys = data.get("sys", {})
        coord = data.get("coord", {})

        record = {
            # ===== identifiers =====
            "city": data.get("name"),
            "city_id": data.get("id"),
            "country": sys.get("country"),
            # ===== timestamps =====
            "event_time": event_time,  # for streaming / window
            "dt": data.get("dt"),  # epoch seconds (API)
            "sunrise": sys.get("sunrise"),
            "sunset": sys.get("sunset"),
            "timezone": data.get("timezone"),
            # ===== location =====
            "coord_lat": coord.get("lat"),
            "coord_lon": coord.get("lon"),
            # ===== weather =====
            "weather_id": weather.get("id"),
            "weather_main": weather.get("main"),
            "weather_description": weather.get("description"),
            "weather_icon": weather.get("icon"),
            # ===== temperature =====
            "temp": main.get("temp"),
            "feels_like": main.get("feels_like"),
            "temp_min": main.get("temp_min"),
            "temp_max": main.get("temp_max"),
            # ===== atmosphere =====
            "pressure": main.get("pressure"),
            "humidity": main.get("humidity"),
            "sea_level": main.get("sea_level"),
            "grnd_level": main.get("grnd_level"),
            "visibility": data.get("visibility"),
            # ===== wind / clouds =====
            "wind_speed": wind.get("speed"),
            "wind_deg": wind.get("deg"),
            "wind_gust": wind.get("gust"),
            "cloudiness": clouds.get("all"),
        }

        return record

    except Exception as e:
        print(f"[ERROR] API error for {city}: {e}")
        return None


def send_to_kafka(record: dict):
    producer.produce(
        topic=TOPIC,
        key=str(record["city"]).encode(),
        value=json.dumps(record).encode(),
    )
    producer.poll(0)


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

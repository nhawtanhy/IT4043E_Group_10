import json
import os
import time
from datetime import datetime

import requests
import boto3
from confluent_kafka import Producer
from botocore.exceptions import ClientError

# ================= CONFIGURATION =================
API_KEY = os.getenv("OPENWEATHER_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "hust-bucket-storage")
TOPIC = "weather_raw"
CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]

# --- SETUP CLIENTS ---
# We initialize these outside the loop for performance

# 1. Kafka Producer
try:
    producer = Producer(
        {"bootstrap.servers":KAFKA_BROKER,
         "socket.timeout.ms":5000}
        # value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # retries=2,
        # request_timeout_ms=5000 # Short timeout so we don't block S3 too long if Kafka is down
    )
    kafka_available = True
except Exception as e:
    print(f"Kafka not available at startup: {e}")
    kafka_available = False

# 2. S3 Client
s3 = boto3.client('s3')

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(
            f"📤 Delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )

def get_s3_key(city, timestamp_str):
    """
    Generates a Hive-style path for ML partitioning:
    path: raw/city=Hanoi/year=2023/month=10/day=27/data.json
    """
    dt = datetime.fromisoformat(timestamp_str)
    return (
        f"raw_weather/"
        f"city={city}/"
        f"year={dt.year}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"{dt.strftime('%H-%M-%S')}.json"
    )


def fetch_weather(city):
    """Fetches data from API."""
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    try:
        res = requests.get(url, timeout=10)
        if res.status_code == 200:
            data = res.json()
            return {
                "city": city,
                "timestamp": datetime.utcnow().isoformat(),
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather": data["weather"][0]["description"],
                "raw": data # Keep full data for ML
            }
        print(f"[WARN] API {res.status_code} for {city}")
    except Exception as e:
        print(f"[ERROR] API Request failed: {e}")
    return None


def send_to_kafka(record):
    """
    Attempt to send to Kafka. 
    Failures here DO NOT stop the program.
    """
    if not kafka_available:
        return
        
    if record:
        producer.produce(
            TOPIC,
            key=record['city'].encode("utf-8"),
            value=json.dumps(record).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)

    producer.flush()


def send_to_s3(record):
    """
    Attempt to upload to S3.
    Failures here DO NOT affect Kafka.
    """
    try:
        s3_key = get_s3_key(record['city'], record['timestamp'])
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(record),
            ContentType='application/json'
        )
        print(f"   💾 S3: Saved {s3_key.split('/')[-1]}")
    except ClientError as e:
        print(f"   ❌ S3 Failed: {e}")


def main():
    print(f"🚀 Dual-Writer Started.")
    print(f"   Target 1: Kafka Topic '{TOPIC}'")
    print(f"   Target 2: S3 Bucket '{S3_BUCKET_NAME}'")
    print("---------------------------------------")

    # while True: uncomment if you want to
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting Batch...")
    if not kafka_available:
        print("Stopped because no kafka. PLease fix")
        return 0
    for city in CITY_LIST:
        print(f"➡️  Processing {city}...")
        record = fetch_weather(city)
        
        if record:
            # THESE TWO RUN INDEPENDENTLY
            send_to_kafka(record)  # Step 1
            send_to_s3(record)     # Step 2
        
    # Ensure any buffered Kafka messages are sent before sleeping
    if kafka_available:
        producer.flush()

if __name__ == "__main__":
    main()
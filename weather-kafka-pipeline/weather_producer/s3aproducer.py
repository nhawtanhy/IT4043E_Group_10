import json
import os
import time
import io
from datetime import datetime, timedelta
# import const
import requests
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# ================= CONFIGURATION =================
API_KEY = os.getenv("OPENWEATHER_API_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "hust-bucket-storage")
TOPIC = "weather_raw"
CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]

# WMO Weather interpretation codes (WW)
WMO_CODE_MAP = {
    0: "clear sky",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast",
    45: "fog",
    48: "depositing rime fog",
    51: "light drizzle",
    53: "moderate drizzle",
    55: "dense drizzle",
    56: "light freezing drizzle",
    57: "dense freezing drizzle",
    61: "slight rain",
    63: "moderate rain",
    65: "heavy rain",
    66: "light freezing rain",
    67: "heavy freezing rain",
    71: "slight snow fall",
    73: "moderate snow fall",
    75: "heavy snow fall",
    77: "snow grains",
    80: "slight rain showers",
    81: "moderate rain showers",
    82: "violent rain showers",
    85: "slight snow showers",
    86: "heavy snow showers",
    95: "thunderstorm",
    96: "thunderstorm with slight hail",
    99: "thunderstorm with heavy hail"
}

# Coordinates for Open-Meteo
CITY_COORDS = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Ho Chi Minh City": {"lat": 10.8231, "lon": 106.6297},
    "Da Nang": {"lat": 16.0544, "lon": 108.2022},
    "Haiphong": {"lat": 20.8449, "lon": 106.6881},
    "Can Tho": {"lat": 10.0452, "lon": 105.7469},
}

s3 = boto3.client('s3')

def fetch_weather_24h(city):
    """
    Fetches hourly data from (NOW - 24 HOURS) up to (NOW - 1 HOUR).
    """
    coords = CITY_COORDS.get(city)
    if not coords:
        return []

    # 1. Calculate the Time Window
    now_dt = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start_dt = now_dt - timedelta(hours=24)
    
    # Format for API (YYYY-MM-DD)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = now_dt.strftime("%Y-%m-%d")

    # 2. Request Data
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "hourly": "temperature_2m,relative_humidity_2m,surface_pressure,cloud_cover,wind_speed_10m,wind_direction_10m,weather_code",
        "start_date": start_str,
        "end_date": end_str
    }

    try:
        res = requests.get(url, params=params, timeout=10)
        if res.status_code != 200:
            print(f"[WARN] History API failed for {city}: {res.text}")
            return []
            
        data = res.json()
        hourly = data.get('hourly', {})
        timestamps = hourly.get('time', [])
        
        records = []
        
        # 3. Iterate and Filter
        for i, t_str in enumerate(timestamps):
            record_dt = datetime.fromisoformat(t_str)
            
            # Keep rows strictly within [Now - 24h, Now)
            # We use '<' instead of '<=' for the upper bound to EXCLUDE current hour
            if start_dt <= record_dt < now_dt:
                
                # Use the WMO map to get text description
                wmo_code = hourly['weather_code'][i]
                desc_str = WMO_CODE_MAP.get(wmo_code, "unknown")

                record = {
                    'city': city,
                    'timestamp': t_str,
                    'description': desc_str, # Using the mapped string now
                    'temp': hourly['temperature_2m'][i],
                    'humidity': hourly['relative_humidity_2m'][i],
                    'pressure': hourly['surface_pressure'][i],
                    'wind_speed': hourly['wind_speed_10m'][i],
                    'wind_deg': hourly['wind_direction_10m'][i],
                    'cloudiness': hourly['cloud_cover'][i],
                    # Defaults
                    'feels_like': None,
                    'temp_min': None,
                    'temp_max': None,
                    'wind_gust': None,
                    'visibility': None
                }
                records.append(record)

    except Exception as e:
        print(f"[ERROR] History fetch error: {e}")
        return []
    
    print(f"Uploading to S3!...")
    file_key = f"weather_data/{city}.parquet"
    new_df = pd.DataFrame(records)
    
    new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], format='mixed')
    # Add 7 hours (GMT+7)
    new_df['timestamp'] = new_df['timestamp'] + timedelta(hours=7)

    try:
        obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
        existing_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'], format='mixed')

        # Merge
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        # Deduplicate (preprocess)
        combined_df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)    
        combined_df.sort_values(by='timestamp', inplace=True)
        
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"   [S3] Creating new Parquet file for {city}")
            combined_df = new_df
            combined_df.sort_values(by='timestamp', inplace=True)
        else:
            print(f"   ❌ S3 Read Error: {e}")
            return

    # Write back to S3
    try:
        out_buffer = io.BytesIO()
        combined_df.to_parquet(out_buffer, index=False, engine='pyarrow')
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=out_buffer.getvalue())
        print(f"   💾 S3: Updated {file_key}")
    except Exception as e:
        print(f"   ❌ S3 Write Failed: {e}")

# def send_to_kafka(record):
#     if not kafka_available or not record:
#         return
#     try:
#         producer.produce(
#             TOPIC,
#             key=record['city'].encode("utf-8"),
#             value=json.dumps(record).encode("utf-8")
#         )
#         producer.poll(0)
#     except Exception as e:
#         print(f"   ❌ Kafka Error: {e}")

# def flatten_current_record(record):
#     raw = record['raw']
#     return {
#         'city': record['city'],
#         'timestamp': record['timestamp'],
#         'description': record['weather'],
#         'temp': raw['main']['temp'],
#         'feels_like': raw['main']['feels_like'],
#         'pressure': raw['main']['pressure'],
#         'humidity': raw['main']['humidity'],
#         'temp_min': raw['main']['temp_min'],
#         'temp_max': raw['main']['temp_max'],
#         'wind_speed': raw['wind'].get('speed', 0),
#         'wind_deg': raw['wind'].get('deg', 0),
#         'wind_gust': raw['wind'].get('gust', 0),
#         'cloudiness': raw['clouds'].get('all', 0),
#         'visibility': raw.get('visibility', 0)
#     }

# def append_to_s3_parquet(new_records_list, city):
#     if not new_records_list:
#         return

#     file_key = f"weather_data/{city}.parquet"
#     new_df = pd.DataFrame(new_records_list)
    
#     # Use mixed format to handle different ISO strings safely
#     new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], format='mixed')
#     new_df['timestamp'] = new_df['timestamp'] + timedelta(hours=7)

#     try:
#         obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
#         existing_df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
#         existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'], format='mixed')

#         combined_df = pd.concat([existing_df, new_df], ignore_index=True)
#         # Drop duplicates by timestamp to keep data clean
#         combined_df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)    
#         combined_df.sort_values(by='timestamp', inplace=True)
        
#         print(f"Existing file found. Total rows: {len(combined_df)}")

#     except ClientError as e:
#         if e.response['Error']['Code'] == "NoSuchKey":
#             print(f"   Creating new Parquet file for {city}")
#             combined_df = new_df
#             combined_df.sort_values(by='timestamp', inplace=True)
#         else:
#             print(f"   ❌ S3 Read Error: {e}")
#             return

#     try:
#         out_buffer = io.BytesIO()
#         combined_df.to_parquet(out_buffer, index=False, engine='pyarrow')
#         s3.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=out_buffer.getvalue())
#         print(f"   💾 S3: Updated {file_key}")
#     except Exception as e:
#         print(f"   ❌ S3 Write Failed: {e}")

def main():
    print(f"Target S3: {S3_BUCKET_NAME}/weather_data/")
    
    while True:
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Starting Batch...")
        for city in CITY_LIST:
            print(f"Processing {city}...")
            # 2. Fetch Rolling 24h History and push to S3
            fetch_weather_24h(city)
        
        # if kafka_available:
        #     producer.flush()
        print("45 minutes sleep...")
        time.sleep(60*45)

if __name__ == "__main__":
    main()
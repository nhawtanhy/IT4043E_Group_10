import io
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from botocore.exceptions import ClientError

# ================= CONFIG =================
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "hust-bucket-storage")
SILVER_PREFIX = "weather_silver"

CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]

CITY_COORDS = {
    "Hanoi": {"lat": 21.0285, "lon": 105.8542},
    "Ho Chi Minh City": {"lat": 10.8231, "lon": 106.6297},
    "Da Nang": {"lat": 16.0544, "lon": 108.2022},
    "Haiphong": {"lat": 20.8449, "lon": 106.6881},
    "Can Tho": {"lat": 10.0452, "lon": 105.7469},
}

s3 = boto3.client("s3")


# ================= CORE =================
def fetch_weather_24h(city: str):
    coords = CITY_COORDS[city]

    # UTC window
    now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    start_utc = now_utc - timedelta(hours=24)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": coords["lat"],
        "longitude": coords["lon"],
        "hourly": (
            "temperature_2m,relative_humidity_2m,surface_pressure,"
            "cloud_cover,wind_speed_10m,wind_direction_10m,weather_code"
        ),
        "start_date": start_utc.strftime("%Y-%m-%d"),
        "end_date": now_utc.strftime("%Y-%m-%d"),
    }

    res = requests.get(url, params=params, timeout=10)
    res.raise_for_status()

    hourly = res.json()["hourly"]

    rows = []
    for i, t in enumerate(hourly["time"]):
        # API trả ISO string → datetime (UTC)
        ts_utc = datetime.fromisoformat(t)

        if start_utc <= ts_utc < now_utc:
            # Convert to GMT+7
            ts_local = ts_utc + timedelta(hours=7)

            rows.append(
                {
                    "city": city,
                    "timestamp": ts_local,  # datetime object
                    "description": str(hourly["weather_code"][i]),
                    "temp": float(hourly["temperature_2m"][i]),
                    "humidity": float(hourly["relative_humidity_2m"][i]),
                    "pressure": float(hourly["surface_pressure"][i]),
                    "wind_speed": float(hourly["wind_speed_10m"][i]),
                    "wind_deg": float(hourly["wind_direction_10m"][i]),
                    "cloudiness": float(hourly["cloud_cover"][i]),
                }
            )

    if not rows:
        print(f"⚠️ No data for {city}")
        return

    df = pd.DataFrame(rows)

    # ================= TIMESTAMP FIX (CRITICAL) =================
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["timestamp"] = df["timestamp"].dt.floor("s")  # ❌ remove nanoseconds

    df.drop_duplicates(subset=["city", "timestamp"], inplace=True)

    key = f"{SILVER_PREFIX}/{city}.parquet"

    # ================= MERGE WITH EXISTING =================
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        old_df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

        old_df["timestamp"] = pd.to_datetime(old_df["timestamp"]).dt.floor("s")

        df = pd.concat([old_df, df], ignore_index=True)
        df.drop_duplicates(subset=["city", "timestamp"], inplace=True)

    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchKey":
            raise

    # ================= WRITE PARQUET (SPARK SAFE) =================
    out = io.BytesIO()

    table = pa.Table.from_pandas(
        df,
        preserve_index=False,
    )

    pq.write_table(
        table,
        out,
        coerce_timestamps="us",  # 🔥 KEY LINE
        allow_truncated_timestamps=True,  # 🔥 avoid ns overflow
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=out.getvalue(),
    )

    print(f"✅ Silver updated: s3://{S3_BUCKET}/{key}")


def main():
    for city in CITY_LIST:
        print(f"🌍 Processing {city}")
        fetch_weather_24h(city)

    print("🎉 Silver batch completed successfully")


if __name__ == "__main__":
    main()

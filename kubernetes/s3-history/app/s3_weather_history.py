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

CITY_COORDS = {
    "An Giang": {"lat": 10.5, "lon": 105.1667},
    "Bac Ninh": {"lat": 21.1833, "lon": 106.05},
    "Ca Mau": {"lat": 9.1769, "lon": 105.15},
    "Cao Bang": {"lat": 22.6667, "lon": 106.25},
    "Can Tho": {"lat": 10.0333, "lon": 105.7833},
    "Da Nang": {"lat": 16.0678, "lon": 108.2208},  # Turan
    "Buon Ma Thuot": {"lat": 12.6667, "lon": 108.05},
    "Dien Bien Phu": {"lat": 21.3833, "lon": 103.0167},
    "Can Gio": {"lat": 10.4167, "lon": 106.9667},
    "Cao Lanh": {"lat": 10.45, "lon": 105.6333},
    "Gia Lai": {"lat": 13.75, "lon": 108.25},
    "Hanoi": {"lat": 21.0245, "lon": 105.8412},
    "Hà Tĩnh": {"lat": 18.3453, "lon": 105.9019},
    "Haiphong": {"lat": 20.8561, "lon": 106.6822},
    "Ho Chi Minh City": {"lat": 10.75, "lon": 106.6667},
    "Hung Yen": {"lat": 20.65, "lon": 106.0667},
    "Khánh Hòa": {"lat": 10.6765, "lon": 105.1903},
    "Lai Chau": {"lat": 22.3997, "lon": 103.4517},
    "Da Lat": {"lat": 11.9465, "lon": 108.4419},
    "Lang Son": {"lat": 21.8333, "lon": 106.7333},
    "Lao Cai": {"lat": 22.4833, "lon": 103.95},
    "Vinh": {"lat": 18.6667, "lon": 105.6667},
    "Ninh Binh": {"lat": 20.2539, "lon": 105.975},
    "Phu Tho": {"lat": 21.3988, "lon": 105.227},
    "Quang Ngai": {"lat": 15.1167, "lon": 108.8},
    "Cam Pha Mines": {"lat": 21.0167, "lon": 107.3},
    "Quảng Trị": {"lat": 16.75, "lon": 107.2},
    "Son La": {"lat": 21.3167, "lon": 103.9},
    "Tay Ninh": {"lat": 11.3, "lon": 106.1},
    "Thai Nguyen": {"lat": 21.5928, "lon": 105.8442},
    "Thanh Hoa": {"lat": 19.8, "lon": 105.7667},
    "Hue": {"lat": 16.4667, "lon": 107.6},
    "Tuyen Quang": {"lat": 21.8233, "lon": 105.2181},
    "Vinh Long": {"lat": 10.25, "lon": 105.9667},
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
        print(f"No data for {city}")
        return

    df = pd.DataFrame(rows)

    # ================= TIMESTAMP FIX (CRITICAL) =================
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["timestamp"] = df["timestamp"].dt.floor("s")  # remove nanoseconds

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
        coerce_timestamps="us",  # KEY LINE
        allow_truncated_timestamps=True,  # avoid ns overflow
    )

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=out.getvalue(),
    )

    print(f"Silver updated: s3://{S3_BUCKET}/{key}")


def main():
    for city in CITY_LIST:
        print(f"Processing {city}")
        fetch_weather_24h(city)

    print("Silver batch completed successfully")


if __name__ == "__main__":
    main()

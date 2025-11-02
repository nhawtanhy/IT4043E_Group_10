import os
import time
from datetime import datetime
from io import BytesIO
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from pymongo import UpdateOne

from connection_api.weather_api import WeatherAPIClient
from connection_db.MinIO import MinioManager
from connection_db.mongo_db import MongoManager


def to_utc_dt_from_local_str(s: str, tz_name: str = "Asia/Bangkok"):
    dt_local = datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo(tz_name))
    return dt_local.astimezone(ZoneInfo("UTC"))


def ensure_unique_index(col):
    try:
        col.create_index(
            [("source", 1), ("city", 1), ("obs_time_utc", 1)],
            unique=True,
            name="uniq_source_city_time",
        )
    except Exception:
        pass


def build_current_docs(api: WeatherAPIClient, city: str):
    docs = []
    try:
        w = api.get_weatherapi_current(city)
        docs.append(
            {
                "source": "weatherapi",
                "city": w["city"],
                "country": w["country"],
                "lat": w["lat"],
                "lon": w["lon"],
                "obs_type": "current",
                "obs_time_utc": to_utc_dt_from_local_str(w["obs_time_utc"]),
                "temp_c": w["temp_c"],
                "humidity": w["humidity"],
                "pressure_hpa": w["pressure_hpa"],
                "wind_speed_ms": (w["wind_kph"] or 0) / 3.6,
                "condition": w["condition"],
                "ingest_ts": datetime.utcnow(),
                "raw": w["raw"],
            }
        )
    except Exception as e:
        print(f"[WARN] WeatherAPI current {city}: {e}")

    try:
        o = api.get_owm_current(city)
        docs.append(
            {
                "source": "openweathermap",
                "city": o["city"],
                "country": o["country"],
                "lat": o["lat"],
                "lon": o["lon"],
                "obs_type": "current",
                "obs_time_utc": datetime.utcfromtimestamp(o["dt"]).replace(
                    tzinfo=ZoneInfo("UTC")
                ),
                "temp_c": o["temp_c"],
                "humidity": o["humidity"],
                "pressure_hpa": o["pressure_hpa"],
                "wind_speed_ms": o["wind_speed"],
                "condition": o["condition"],
                "ingest_ts": datetime.utcnow(),
                "raw": o["raw"],
            }
        )
    except Exception as e:
        print(f"[WARN] OWM current {city}: {e}")

    return docs


def upsert_mongo(col, docs):
    ops = []
    for d in docs:
        key = {
            "source": d["source"],
            "city": d["city"],
            "obs_time_utc": d["obs_time_utc"],
        }
        ops.append(UpdateOne(key, {"$set": d}, upsert=True))
    if ops:
        res = col.bulk_write(ops, ordered=False)
        return (res.upserted_count or 0, res.modified_count or 0, len(ops))
    return (0, 0, 0)


def write_parquet_to_minio(minio_mgr: MinioManager, bucket: str, docs):
    if not docs:
        return 0, []

    import os
    from datetime import datetime

    df = pd.DataFrame(docs)

    # Ép kiểu datetime về UTC, loại bỏ tz để ghi Parquet ổn định
    # Chấp tất cả: datetime tz-aware, naive, hoặc string ISO
    dt = pd.to_datetime(df["obs_time_utc"], utc=True, errors="coerce")
    if dt.isna().any():
        # bỏ các dòng lỗi datetime để tránh hỏng batch
        df = df[~dt.isna()].copy()
        dt = dt[~dt.isna()]
    # chuẩn UTC và bỏ tz (naive UTC) để partition theo ngày/giờ
    df["obs_time_utc"] = dt.dt.tz_convert("UTC").dt.tz_localize(None)

    # Partition theo UTC
    df["dt"] = df["obs_time_utc"].dt.strftime("%Y-%m-%d")
    df["hour"] = df["obs_time_utc"].dt.strftime("%H")

    uploaded = []
    now_tag = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

    for (source, dt_s, hour_s), g in df.groupby(["source", "dt", "hour"]):
        buf = BytesIO()
        g.drop(columns=["dt", "hour"]).to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)

        object_name = f"weather/source={source}/dt={dt_s}/hour={hour_s}/batch_{now_tag}_{uuid4().hex[:8]}.parquet"

        tmp_path = f"/tmp/{uuid4().hex}.parquet"
        with open(tmp_path, "wb") as f:
            f.write(buf.read())
        minio_mgr.upload_file(bucket, tmp_path, object_name)
        try:
            os.remove(tmp_path)
        except Exception:
            pass

        uploaded.append(object_name)

    return len(uploaded), uploaded


def main_loop():
    load_dotenv()
    cities = [c.strip() for c in os.getenv("CITIES", "Hanoi").split(",") if c.strip()]
    interval = int(os.getenv("INGEST_INTERVAL_SEC", "300"))
    bucket = os.getenv("WEATHER_BUCKET", "weather-raw-prod")

    api = WeatherAPIClient()
    mongo = MongoManager()
    mongo.connect()
    col = mongo.get_db("weather")["observations"]
    ensure_unique_index(col)

    minio_mgr = MinioManager()
    minio_mgr.create_bucket(bucket)

    print(f"Start loop | cities={cities} | interval={interval}s | bucket={bucket}")
    try:
        while True:
            start = datetime.utcnow()
            all_docs = []
            for city in cities:
                all_docs.extend(build_current_docs(api, city))

            upserts, mods, ops = upsert_mongo(col, all_docs)
            n_files, keys = write_parquet_to_minio(minio_mgr, bucket, all_docs)

            print(
                f"[{start.isoformat()}] mongo upsert={upserts} modified={mods} ops={ops} | parquet_files={n_files}"
            )
            if n_files:
                print(
                    "  uploaded:",
                    "; ".join(keys[:5]) + (" ..." if len(keys) > 5 else ""),
                )

            elapsed = (datetime.utcnow() - start).total_seconds()
            sleep_s = max(1, interval - int(elapsed))
            time.sleep(sleep_s)
    except KeyboardInterrupt:
        print("Stop requested.")
    finally:
        mongo.close()


if __name__ == "__main__":
    main_loop()

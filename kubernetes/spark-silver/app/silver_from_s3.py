import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, StringType

# =====================================================
# Config
# =====================================================
APP_NAME = os.getenv("APP_NAME", "weather-silver")

S3_INPUT = os.getenv(
    "S3_INPUT",
    "s3a://hust-bucket-storage/weather_silver/",
)

SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "/data/silver/weather",
)

# =====================================================
# Spark Session
# =====================================================
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print(f"📥 Reading from: {S3_INPUT}")
print(f"📤 Writing silver to: {SILVER_PATH}")

# =====================================================
# 1️⃣ READ PARQUET (ALREADY CLEAN FROM S3)
# =====================================================
df = spark.read.parquet(S3_INPUT)

# =====================================================
# 2️⃣ SELECT + CLEAN (ONLY EXISTING COLUMNS)
# =====================================================
silver = (
    df.select(
        col("city").cast(StringType()).alias("city"),
        col("timestamp"),  # already timestamp[us]
        col("description").cast(StringType()).alias("description"),
        col("temp").cast(DoubleType()).alias("temp"),
        col("humidity").cast(DoubleType()).alias("humidity"),
        col("pressure").cast(DoubleType()).alias("pressure"),
        col("wind_speed").cast(DoubleType()).alias("wind_speed"),
        col("wind_deg").cast(DoubleType()).alias("wind_deg"),
        col("cloudiness").cast(DoubleType()).alias("cloudiness"),
    )
    .filter(col("city").isNotNull())
    .filter(col("timestamp").isNotNull())
    .filter(col("temp").isNotNull())
    .dropDuplicates(["city", "timestamp"])
)

# =====================================================
# 3️⃣ WRITE SILVER (PVC / LOCAL)
# =====================================================
silver.write.mode("overwrite").parquet(SILVER_PATH)

print("✅ Silver job completed successfully")
spark.stop()

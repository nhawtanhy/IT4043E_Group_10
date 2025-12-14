import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    when,
)
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window

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

spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print(f"📥 Reading from: {S3_INPUT}")
print(f"📤 Writing silver to: {SILVER_PATH}")

# =====================================================
# 1️⃣ READ PARQUET (Clean Bronze-from-S3)
# =====================================================

files = [
    f"{S3_INPUT}/Hanoi.parquet",
    f"{S3_INPUT}/Da Nang.parquet",
    f"{S3_INPUT}/Haiphong.parquet",
    f"{S3_INPUT}/Can Tho.parquet",
    f"{S3_INPUT}/Ho Chi Minh City.parquet",
]
df = spark.read.parquet(*files)

# =====================================================
# 2️⃣ BASIC CLEAN + TYPE NORMALIZATION
# =====================================================

base = (
    df.select(
        col("city").cast(StringType()).alias("city"),
        col("timestamp"),
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
# 3️⃣ BUSINESS LOGIC (DERIVED FEATURES)
# =====================================================

# 🌡️ Temperature category (business rule)
base = base.withColumn(
    "temp_category",
    when(col("temp") < 20, "cold").when(col("temp") < 30, "warm").otherwise("hot"),
)

# =====================================================
# 4️⃣ WINDOW AGGREGATIONS (INTERMEDIATE SPARK SKILL)
# =====================================================

# Rolling 24h average temperature per city
w_24h = (
    Window.partitionBy("city")
    .orderBy(col("timestamp").cast("long"))
    .rowsBetween(-23, 0)
)

silver = base.withColumn(
    "avg_temp_24h",
    avg("temp").over(w_24h),
)

# =====================================================
# 5️⃣ PERFORMANCE OPTIMIZATION
# =====================================================

# Repartition by city → better downstream joins
silver = silver.repartition("city")

# Cache because Silver is reused by Gold
silver.cache()

# Materialize cache (important for teaching/demo)
silver.count()
print("🔍 Silver count:", silver.count())

silver.groupBy("city").count().show()

# =====================================================
# 6️⃣ WRITE SILVER (PARTITIONED)
# =====================================================
(silver.write.mode("overwrite").partitionBy("city").parquet(SILVER_PATH))

print("✅ Silver job completed successfully")
spark.stop()

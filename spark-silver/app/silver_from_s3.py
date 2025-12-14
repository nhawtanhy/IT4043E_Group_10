import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, when
from pyspark.sql.types import DoubleType, StringType
from pyspark.sql.window import Window

# =====================================================
# Config
# =====================================================

APP_NAME = os.getenv("APP_NAME", "weather-silver")

S3_INPUT = os.getenv(
    "S3_INPUT",
    "s3a://hust-bucket-storage/weather_silver",
)

SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "/data/silver/weather",
)

# =====================================================
# CITY LIST (HARD-CODED)
# =====================================================

CITY_LIST = [
    "An Giang",
    "Bac Ninh",
    "Buon Ma Thuot",
    "Ca Mau",
    "Cam Pha Mines",
    "Can Gio",
    "Can Tho",
    "Cao Bang",
    "Cao Lanh",
    "Da Lat",
    "Da Nang",
    "Dien Bien Phu",
    "Gia Lai",
    "Haiphong",
    "Hanoi",
    "Hà Tĩnh",
    "Ho Chi Minh City",
    "Hue",
    "Hung Yen",
    "Khánh Hòa",
    "Lai Chau",
    "Lang Son",
    "Lao Cai",
    "Ninh Binh",
    "Phu Tho",
    "Quang Ngai",
    "Quảng Trị",
    "Son La",
    "Tay Ninh",
    "Thai Nguyen",
    "Thanh Hoa",
    "Tuyen Quang",
    "Vinh",
    "Vinh Long",
]

# =====================================================
# Spark Session
# =====================================================

spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"📥 Reading from S3: {S3_INPUT}")
print(f"📤 Writing Silver to: {SILVER_PATH}")

# =====================================================
# 1️⃣ READ PARQUET FILES (ALL CITIES)
# =====================================================

from pyspark.sql.utils import AnalysisException

valid_files = []
for city in CITY_LIST:
    path = f"{S3_INPUT}/{city}.parquet"
    try:
        spark.read.parquet(path).limit(1)
        valid_files.append(path)
    except AnalysisException:
        print(f"⚠️ Missing file: {path}")

df = spark.read.parquet(*valid_files)

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

base = base.withColumn(
    "temp_category",
    when(col("temp") < 20, "cold").when(col("temp") < 30, "warm").otherwise("hot"),
)

# =====================================================
# 4️⃣ WINDOW AGGREGATION (24H ROLLING AVG)
# =====================================================

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

silver = silver.repartition("city")
silver.cache()

count = silver.count()
print(f"🔍 Silver record count: {count}")

silver.groupBy("city").count().show(truncate=False)

# =====================================================
# 6️⃣ WRITE SILVER (PARTITIONED BY CITY)
# =====================================================

(silver.write.mode("overwrite").partitionBy("city").parquet(SILVER_PATH))

print("✅ Silver job completed successfully")
spark.stop()
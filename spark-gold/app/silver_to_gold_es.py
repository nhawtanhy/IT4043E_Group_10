import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    expr,
    lit,
    when,
    window,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min

# =====================================================
# Config
# =====================================================
APP_NAME = os.getenv("APP_NAME", "silver-to-gold-es")

SILVER_PATH = os.getenv("SILVER_PATH", "/data/silver/weather")

STATE_PATH = os.getenv("STATE_PATH", "/checkpoint/gold_state")
STATE_FILE = "last_ts.txt"

ES_NODES = os.getenv("ES_NODES")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
ES_INDEX = os.getenv("ES_INDEX", "weather-gold-hourly-v2")


# =====================================================
# Spark Session
# =====================================================
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# =====================================================
# Hadoop FS
# =====================================================
hadoop = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop)

silver_path = spark._jvm.org.apache.hadoop.fs.Path(SILVER_PATH)
state_dir = spark._jvm.org.apache.hadoop.fs.Path(STATE_PATH)
state_file = spark._jvm.org.apache.hadoop.fs.Path(f"{STATE_PATH}/{STATE_FILE}")


# =====================================================
# 1️⃣ Check Silver existence
# =====================================================
if not fs.exists(silver_path):
    print("❌ Silver path not found. Exit.")
    spark.stop()
    raise SystemExit(0)


# =====================================================
# 2️⃣ Read Silver
# =====================================================
df = spark.read.parquet(SILVER_PATH)

# (Optional) guard: ensure timestamp is timestamp type (depends on your Silver)
# If your Silver already stores timestamp as TimestampType, keep it.
# If it's string, uncomment:
# df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

df = df.filter(col("timestamp").isNotNull()).filter(col("city").isNotNull())


# =====================================================
# 3️⃣ Load incremental state
# =====================================================
last_ts = None
if fs.exists(state_file):
    f = fs.open(state_file)
    last_ts = f.readUTF()
    f.close()

if last_ts:
    df = df.filter(col("timestamp") > last_ts)

if df.rdd.isEmpty():
    print("ℹ️ No new Silver data. Exit.")
    spark.stop()
    raise SystemExit(0)


# =====================================================
# 4️⃣ HOURLY BASE AGGREGATIONS
#    - adapt to schema: wind_gust may not exist in Silver
# =====================================================
has_wind_gust = "wind_gust" in df.columns

agg_exprs = [
    # Temperature
    avg("temp").alias("avg_temp"),
    spark_min("temp").alias("min_temp"),
    spark_max("temp").alias("max_temp"),
    avg("avg_temp_24h").alias("avg_temp_24h"),
    # Humidity
    avg("humidity").alias("avg_humidity"),
    spark_min("humidity").alias("min_humidity"),
    spark_max("humidity").alias("max_humidity"),
    # Pressure
    avg("pressure").alias("avg_pressure"),
    spark_min("pressure").alias("min_pressure"),
    spark_max("pressure").alias("max_pressure"),
    # Wind
    avg("wind_speed").alias("avg_wind_speed"),
    spark_max("wind_speed").alias("max_wind_speed"),
    avg("wind_deg").alias("avg_wind_deg"),
    # Cloud
    avg("cloudiness").alias("avg_cloudiness"),
    count("*").alias("records"),
]

if has_wind_gust:
    agg_exprs.append(spark_max("wind_gust").alias("max_wind_gust"))

gold_base = (
    df.groupBy(window(col("timestamp"), "1 hour"), col("city"))
    .agg(*agg_exprs)
    .withColumn("temp_range", col("max_temp") - col("min_temp"))
    .withColumn("humidity_range", col("max_humidity") - col("min_humidity"))
    .withColumn("pressure_range", col("max_pressure") - col("min_pressure"))
    .withColumn("temp_vs_24h", col("avg_temp") - col("avg_temp_24h"))
)

# If wind_gust doesn't exist, create a consistent column for downstream usage
if not has_wind_gust:
    gold_base = gold_base.withColumn("max_wind_gust", lit(None).cast("double"))

gold_base = gold_base.select(
    col("city"),
    col("window.start").alias("@timestamp"),
    # Temp
    "avg_temp",
    "min_temp",
    "max_temp",
    "temp_range",
    "avg_temp_24h",
    "temp_vs_24h",
    # Humidity
    "avg_humidity",
    "min_humidity",
    "max_humidity",
    "humidity_range",
    # Pressure
    "avg_pressure",
    "min_pressure",
    "max_pressure",
    "pressure_range",
    # Wind
    "avg_wind_speed",
    "max_wind_speed",
    "max_wind_gust",
    "avg_wind_deg",
    # Cloud
    "avg_cloudiness",
    "records",
)


# =====================================================
# 5️⃣ DOMINANT WEATHER DESCRIPTION
# =====================================================
desc_dist = df.groupBy(
    window(col("timestamp"), "1 hour"),
    col("city"),
    col("description"),
).count()

dominant_desc = (
    desc_dist.withColumn(
        "rank",
        expr("row_number() over (partition by window, city order by count desc)"),
    )
    .filter(col("rank") == 1)
    .select(
        col("city"),
        col("window.start").alias("@timestamp"),
        col("description").alias("dominant_weather_desc"),
    )
)

gold = gold_base.join(dominant_desc, on=["city", "@timestamp"], how="left")


# =====================================================
# 6️⃣ SEMANTIC WIND DIRECTION
# =====================================================
gold = gold.withColumn(
    "wind_direction",
    when((col("avg_wind_deg") >= 337.5) | (col("avg_wind_deg") < 22.5), "N")
    .when(col("avg_wind_deg") < 67.5, "NE")
    .when(col("avg_wind_deg") < 112.5, "E")
    .when(col("avg_wind_deg") < 157.5, "SE")
    .when(col("avg_wind_deg") < 202.5, "S")
    .when(col("avg_wind_deg") < 247.5, "SW")
    .when(col("avg_wind_deg") < 292.5, "W")
    .otherwise("NW"),
)


# =====================================================
# 7️⃣ RULE-BASED FLAGS (DASHBOARD ALERTS)
# =====================================================
gold = (
    gold.withColumn("unstable_temp", (col("temp_range") > 5).cast("boolean"))
    .withColumn("unstable_humidity", (col("humidity_range") > 20).cast("boolean"))
    .withColumn("pressure_drop", (col("pressure_range") > 3).cast("boolean"))
    .withColumn("strong_wind", (col("max_wind_speed") > 10).cast("boolean"))
    # If max_wind_gust is null (no wind_gust in Silver), this becomes false
    .withColumn(
        "wind_gust_event",
        when(col("max_wind_gust").isNull(), lit(False))
        .otherwise((col("max_wind_gust") > 15))
        .cast("boolean"),
    )
    .withColumn("data_gap", (col("records") < 50).cast("boolean"))
)


# =====================================================
# 8️⃣ FIX @timestamp FORMAT FOR ELASTICSEARCH
# =====================================================
gold = gold.withColumn(
    "@timestamp", date_format(col("@timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
)


# =====================================================
# 9️⃣ WRITE TO ELASTICSEARCH
# =====================================================
es_options = {
    "es.nodes": f"http://{ES_NODES}",
    "es.port": ES_PORT,
    "es.resource": f"{ES_INDEX}/_doc",
    "es.nodes.wan.only": "true",
    "es.net.ssl": "false",
    "es.net.http.auth.user": ES_USER,
    "es.net.http.auth.pass": ES_PASS,
}

(
    gold.write.format("org.elasticsearch.spark.sql")
    .mode("append")
    .options(**es_options)
    .save()
)


# =====================================================
# 🔟 UPDATE INCREMENTAL STATE
# =====================================================
new_ts = gold.select("@timestamp").agg(spark_max(col("@timestamp"))).collect()[0][0]

fs.mkdirs(state_dir)
out = fs.create(state_file, True)
out.writeUTF(str(new_ts))
out.close()

print("✅ Gold hourly weather analytics (FULL) written successfully")

spark.stop()

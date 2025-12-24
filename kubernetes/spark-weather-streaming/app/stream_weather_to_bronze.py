import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, window
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# =====================================================
# Config
# =====================================================
APP_NAME = os.getenv("APP_NAME", "weather-streaming-to-bronze")

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP", "kafka-kafka-bootstrap.data.svc.cluster.local:9092"
)
TOPIC = os.getenv("KAFKA_TOPIC", "weather-raw")

CHECKPOINT = os.getenv("CHECKPOINT", "/checkpoint/weather-raw")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/checkpoint/bronze_parquet")

MAX_OFFSETS = os.getenv("MAX_OFFSETS", "5000")
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARN")

# Elasticsearch
ES_NODES = os.getenv(
    "ES_NODES"
)  # e.g. es-weather-es-http.observability.svc.cluster.local
ES_PORT = os.getenv("ES_PORT", "9200")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
ES_INDEX = os.getenv("ES_INDEX", "weather-agg-10m")

# =====================================================
# Spark Session
# =====================================================
spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel(LOG_LEVEL)

# =====================================================
# Schema
# =====================================================
schema = StructType(
    [
        StructField("city", StringType(), True),
        StructField("temp", DoubleType(), True),
    ]
)

# =====================================================
# 1️⃣ READ KAFKA (RAW STREAM)
# =====================================================
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS value", "timestamp")
    .withColumn("json", from_json(col("value"), schema))
    .select(
        col("timestamp").alias("event_time"),
        col("json.city").alias("city"),
        col("json.temp").alias("temp"),
    )
)

# =====================================================
# 2️⃣ RAW → BRONZE (IMMUTABLE STORAGE)
# =====================================================
q_bronze = (
    parsed.writeStream.format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT + "/bronze")
    .start()
)

# =====================================================
# 3️⃣ STATEFUL STREAM (WINDOW + WATERMARK)
# =====================================================
stateful = (
    parsed.withWatermark("event_time", "15 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("city"),
    )
    .agg(avg("temp").alias("avg_temp"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("avg_temp"),
    )
)

# =====================================================
# 4️⃣ STATEFUL → ELASTICSEARCH (STREAMING)
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

q_stateful_es = (
    stateful.writeStream.outputMode("append")  # window đóng → ghi
    .format("org.elasticsearch.spark.sql")
    .options(**es_options)
    .option("checkpointLocation", CHECKPOINT + "/stateful-es")
    .start()
)

# =====================================================
# Run
# =====================================================
spark.streams.awaitAnyTermination()

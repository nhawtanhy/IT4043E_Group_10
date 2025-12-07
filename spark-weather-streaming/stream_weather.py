# stream_weather.py
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# ======================
# ENV / CONFIG
# ======================
APP_NAME = os.getenv("APP_NAME", "weather-streaming")

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP", "kafka-kafka-bootstrap.data.svc.cluster.local:9092"
)
TOPIC = os.getenv("KAFKA_TOPIC", "weather-raw")  # ✅ đúng topic của bạn

CHECKPOINT = os.getenv("CHECKPOINT", "/checkpoint/weather-raw")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/checkpoint/bronze_parquet")

MAX_OFFSETS = os.getenv("MAX_OFFSETS", "5000")
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARN")

# Elasticsearch (ECK service)
ES_NODES = os.getenv("ES_NODES", "es-weather-es-http.observability.svc.cluster.local")
ES_PORT = os.getenv("ES_PORT", "9200")

# ⚠️ NÊN set qua env/secret, đừng hardcode vào code
ES_USER = os.getenv("ES_USER", "elastic")
ES_PASS = os.getenv("ES_PASS", "")

# Index (giữ như Kibana bạn đang query: weather-raw)
ES_INDEX = os.getenv("ES_INDEX", "weather-raw")

# ======================
# SPARK SESSION
# ======================
spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel(LOG_LEVEL)

# ======================
# SCHEMA
# ======================
schema = StructType(
    [
        StructField("city", StringType(), True),
        StructField("temp", DoubleType(), True),
    ]
)

# ======================
# KAFKA SOURCE
# ======================
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
    .select(col("timestamp").alias("@timestamp"), col("json.*"))
)

# ======================
# SINK 1: CONSOLE (VERIFY)
# ======================
# q1 = (
#     parsed.writeStream.format("console")
#     .outputMode("append")
#     .option("truncate", "false")
#     .option("checkpointLocation", CHECKPOINT + "/console")
#     .start()
# )

# # ======================
# # SINK 2: BRONZE PARQUET
# # ======================
# q2 = (
#     parsed.writeStream.format("parquet")
#     .outputMode("append")
#     .option("path", BRONZE_PATH)
#     .option("checkpointLocation", CHECKPOINT + "/bronze")
#     .start()
# )

# ======================
# SINK 3: ELASTICSEARCH (ECK + HTTPS)
# ======================
# Notes:
# - es.nodes.wan.only=true: tránh connector tự sniff pod IP rồi fail TLS/DNS
# - self-signed cert: allow self signed + disable hostname verification
es_options = {
    "es.nodes": f"http://{ES_NODES}",
    "es.port": ES_PORT,
    "es.resource": f"{ES_INDEX}/_doc",
    "es.nodes.wan.only": "true",
    "es.net.ssl": "false",
    "es.net.http.auth.user": ES_USER,
    "es.net.http.auth.pass": ES_PASS,
}

q3 = (
    parsed.writeStream.format("org.elasticsearch.spark.sql")
    .outputMode("append")
    .options(**es_options)
    .option("checkpointLocation", CHECKPOINT + "/es")
    .start()
)

# ======================
# BLOCK
# ======================
spark.streams.awaitAnyTermination()

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

APP_NAME = os.getenv("APP_NAME", "weather-streaming-to-bronze")

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP", "kafka-kafka-bootstrap.data.svc.cluster.local:9092"
)
TOPIC = os.getenv("KAFKA_TOPIC", "weather-raw")

CHECKPOINT = os.getenv("CHECKPOINT", "/checkpoint/weather-raw")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/checkpoint/bronze_parquet")

MAX_OFFSETS = os.getenv("MAX_OFFSETS", "5000")
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARN")

spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel(LOG_LEVEL)

schema = StructType(
    [
        StructField("city", StringType(), True),
        StructField("temp", DoubleType(), True),
    ]
)

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


q_bronze = (
    parsed.writeStream.format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT + "/bronze")
    .start()
)


spark.streams.awaitAnyTermination()

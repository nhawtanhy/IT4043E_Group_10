import json

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import FloatType, StringType, StructField, StructType

KAFKA_BROKER = "localhost:9092"
TOPIC = "weather_raw"

ES_URL = "https://localhost:9200/weather-data/_bulk"
ES_USER = "elastic"
ES_PASS = "VqCSeZ7ZSRAYbzQhTTK0"

# Ignore self-signed SSL
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

weather_schema = StructType(
    [
        StructField("city", StringType()),
        StructField("timestamp", StringType()),
        StructField("temperature", FloatType()),
        StructField("humidity", FloatType()),
        StructField("weather", StringType()),
    ]
)

spark = (
    SparkSession.builder.appName("WeatherStreamToES")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

df_parsed = (
    df_raw.selectExpr("CAST(value AS STRING) AS json_data")
    .select(from_json(col("json_data"), weather_schema).alias("data"))
    .select("data.*")
)


def write_to_es(batch_df, batch_id):
    rows = batch_df.collect()
    print(f">>> BATCH {batch_id}, ROWS = {len(rows)}")

    if not rows:
        return

    # Build ES bulk body
    bulk_data = ""
    for row in rows:
        meta = '{"index":{}}\n'
        doc = json.dumps(row.asDict()) + "\n"
        bulk_data += meta + doc

    # Send to Elasticsearch
    resp = requests.post(
        ES_URL,
        headers={"Content-Type": "application/x-ndjson"},
        data=bulk_data,
        auth=(ES_USER, ES_PASS),
        verify=False,
    )

    print(">>> ES RESPONSE:", resp.status_code, resp.text[:200])


# DEBUG console
df_parsed.writeStream.outputMode("append").format("console").option(
    "truncate", False
).start()

(
    df_parsed.writeStream.foreachBatch(write_to_es)
    .option("checkpointLocation", "./spark_weather_checkpoint")
    .start()
    .awaitTermination()
)

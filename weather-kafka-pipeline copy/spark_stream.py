from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import FloatType, StringType, StructField, StructType

# ================================
# Config
# ================================
KAFKA = "kafka:9092"
TOPIC = "weather_raw"

ES_INDEX = "weather-data"

schema = StructType(
    [
        StructField("city", StringType()),
        StructField("timestamp", StringType()),
        StructField("temperature", FloatType()),
        StructField("humidity", FloatType()),
        StructField("weather", StringType()),
    ]
)


# ================================
# foreachBatch — xử lý từng batch
# ================================
def foreach_batch(df, batch_id):
    print(f"\n========== BATCH {batch_id} ==========")

    if df.rdd.isEmpty():
        print("[Batch empty] Không có dữ liệu mới.")
        return

    # In trước 10 dòng để debug
    df.show(10, truncate=False)

    try:
        (
            df.write.format("org.elasticsearch.spark.sql")
            .option("es.nodes", "elasticsearch")
            .option("es.port", "9200")
            .option("es.nodes.wan.only", "true")  # quan trọng trong Docker Desktop
            # .option("es.resource", f"{ES_INDEX}/_doc")
            .option("es.resource", f"{ES_INDEX}")
            .mode("append")
            .save()
        )

        print(f"[OK] Saved batch {batch_id} → Elasticsearch")

    except Exception as e:
        print(f"[ERROR saving batch {batch_id} → ES]: {e}")


# ================================
# Main Spark App
# ================================
spark = SparkSession.builder.appName("WeatherSparkStreamer").getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ================================
# Read STREAM from Kafka
# ================================
df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")  # không đọc lại từ đầu
    .load()
)

# value -> string JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) AS json")

# Parse JSON theo schema
df_parsed = df_json.select(from_json(col("json"), schema).alias("data")).select(
    "data.*"
)

# ================================
# Start Streaming Query
# ================================
(
    df_parsed.writeStream.outputMode("append")
    .foreachBatch(foreach_batch)
    .option("checkpointLocation", "/checkpoint")
    .start()
    .awaitTermination()
)

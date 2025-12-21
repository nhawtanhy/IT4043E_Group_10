import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, date_format, concat_ws, lower, regexp_replace
from pyspark.sql.types import FloatType, StringType, IntegerType, StructField, StructType

KAFKA = "kafka:9092"
TOPIC = "weather_raw"
ES_INDEX = "weather-data"

# Define schema
schema = StructType([
    StructField("city", StringType()),
    StructField("timestamp", StringType()),
    StructField("description", StringType()), 
    StructField("temp", FloatType()),
    StructField("pressure", FloatType()),
    StructField("humidity", FloatType()),
    StructField("wind_speed", FloatType()),
    StructField("wind_deg", FloatType()),
    StructField("wind_gust", FloatType()),
    StructField("cloudiness", IntegerType())
])

# ================================
# foreachBatch — Write to ES
# ================================
def foreach_batch(df, batch_id):
    print(f"\n========== BATCH {batch_id} ==========")

    if df.rdd.isEmpty():
        print("[Batch empty] No new data.")
        return

    # Debug: Print to console
    df.show(5, truncate=False)
    
    # es_id = lowered city name
    df_with_id = df.withColumn("es_id", 
        concat_ws( 
                    "_2",
                  lower(regexp_replace(col("city"), " ", "_"))
        )
    )
    
    try:
        (
            df_with_id.write.format("org.elasticsearch.spark.sql")
            .option("es.nodes", "elasticsearch")
            .option("es.port", "9200")
            .option("es.nodes.wan.only", "true")
            .option("es.resource", f"{ES_INDEX}")
            .option("es.mapping.id", "es_id")       
            .option("es.mapping.exclude", "es_id")  
            .option("es.write.operation", "upsert") 
            .mode("append")
            .save()
        )
        print(f"[OK] Saved batch {batch_id} → Elasticsearch")
    except Exception as e:
        print(f"[ERROR saving batch {batch_id} → ES]: {e}")

# Main Spark App
spark = SparkSession.builder.appName("WeatherSparkStreamer").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read STREAM from Kafka
df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) AS json")
df_parsed = df_json.select(from_json(col("json"), schema).alias("data")).select("data.*")

# ================================
# 2. FORMATTING
# (Standardizing timestamp string)
# ================================
df_formatted = df_parsed.withColumn(
    "timestamp",
    date_format(to_timestamp(col("timestamp")), "yyyy-MM-dd HH:mm:ss")
)

# ================================
# Start Streaming Query
# ================================
(
    df_formatted.writeStream.outputMode("update")
    .foreachBatch(foreach_batch)
    .option("checkpointLocation", "/checkpoint")
    .trigger(processingTime='5 seconds')
    .start()
    .awaitTermination()
)
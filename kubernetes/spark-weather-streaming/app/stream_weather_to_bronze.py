import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat_ws,
    count,
    date_format,
    from_json,
    from_unixtime,
    to_timestamp,
    unix_timestamp,
    window,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# =====================================================
# CONFIG
# =====================================================
APP_NAME = os.getenv("APP_NAME", "weather-streaming-to-bronze")

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP",
    "kafka-kafka-bootstrap.data.svc.cluster.local:9092",
)
TOPIC = os.getenv("KAFKA_TOPIC", "weather-raw")

CHECKPOINT_BASE = os.getenv("CHECKPOINT", "/checkpoint/weather-raw")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/checkpoint/bronze_parquet")

ES_NODES = os.getenv("ES_NODES")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
ES_INDEX = os.getenv("ES_INDEX", "weather-agg-10m-v4")

# =====================================================
# SPARK
# =====================================================
spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# SCHEMA (FULL – RAW EVENT)
# =====================================================
schema = StructType(
    [
        StructField("city", StringType()),
        StructField("city_id", IntegerType()),
        StructField("country", StringType()),
        StructField("event_time", StringType()),  # kept but NOT used for watermark
        StructField("dt", IntegerType()),  # 🔑 REAL EVENT TIME
        StructField("sunrise", IntegerType()),
        StructField("sunset", IntegerType()),
        StructField("timezone", IntegerType()),
        StructField("coord_lat", DoubleType()),
        StructField("coord_lon", DoubleType()),
        StructField("weather_id", IntegerType()),
        StructField("weather_main", StringType()),
        StructField("weather_description", StringType()),
        StructField("weather_icon", StringType()),
        StructField("temp", DoubleType()),
        StructField("feels_like", DoubleType()),
        StructField("temp_min", DoubleType()),
        StructField("temp_max", DoubleType()),
        StructField("pressure", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("sea_level", DoubleType()),
        StructField("grnd_level", DoubleType()),
        StructField("visibility", IntegerType()),
        StructField("wind_speed", DoubleType()),
        StructField("wind_deg", DoubleType()),
        StructField("wind_gust", DoubleType()),
        StructField("cloudiness", IntegerType()),
    ]
)

# =====================================================
# 1️⃣ READ KAFKA
# =====================================================
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = raw.selectExpr("CAST(value AS STRING) AS value").withColumn(
    "json", from_json(col("value"), schema)
)

# =====================================================
# 2️⃣ BRONZE STREAM (RAW, IMMUTABLE, FULL PAYLOAD)
# =====================================================
bronze_df = parsed.select("json.*")

q_bronze = (
    bronze_df.writeStream.format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
    .start()
)

# =====================================================
# 3️⃣ CLEAN STREAM (EVENT-TIME THỰC TỪ dt)
# =====================================================
clean = (
    parsed.select(
        col("json.city").alias("city"),
        to_timestamp(from_unixtime(col("json.dt"))).alias("event_time"),
        col("json.temp").alias("temp"),
        col("json.humidity").alias("humidity"),
        col("json.pressure").alias("pressure"),
        col("json.wind_speed").alias("wind_speed"),
    )
    .filter(col("city").isNotNull())
    .filter(col("event_time").isNotNull())
    .filter(col("temp").isNotNull())
)


# =====================================================
# 4️⃣ WINDOW + WATERMARK (ADVANCED GOLD METRICS)
# =====================================================
stateful = (
    clean.withWatermark("event_time", "15 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("city"),
    )
    .agg(
        # Temperature
        avg("temp").alias("avg_temp"),
        spark_min("temp").alias("min_temp"),
        spark_max("temp").alias("max_temp"),
        # Humidity
        avg("humidity").alias("avg_humidity"),
        spark_min("humidity").alias("min_humidity"),
        spark_max("humidity").alias("max_humidity"),
        # Wind
        avg("wind_speed").alias("avg_wind_speed"),
        spark_max("wind_speed").alias("max_wind_speed"),
        # Volume
        count("*").alias("records"),
    )
    .withColumn(
        "temp_range",
        col("max_temp") - col("min_temp"),
    )
    .select(
        col("window.start").alias("@timestamp"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("city"),
        col("avg_temp"),
        col("min_temp"),
        col("max_temp"),
        col("temp_range"),
        col("avg_humidity"),
        col("min_humidity"),
        col("max_humidity"),
        col("avg_wind_speed"),
        col("max_wind_speed"),
        col("records"),
    )
    .withColumn(
        "doc_id",
        concat_ws(
            "_",
            col("city"),
            unix_timestamp(col("window_start")),
        ),
    )
)

# =====================================================
# 5️⃣ WRITE TO ES (IDEMPOTENT UPSERT)
# =====================================================
es_options = {
    "es.nodes": f"http://{ES_NODES}",
    "es.port": ES_PORT,
    "es.resource": f"{ES_INDEX}/_doc",
    "es.nodes.wan.only": "true",
    "es.net.ssl": "false",
    "es.net.http.auth.user": ES_USER,
    "es.net.http.auth.pass": ES_PASS,
    "es.mapping.id": "doc_id",
    "es.write.operation": "upsert",
}


def write_es(batch_df, batch_id):
    (
        batch_df
        # 🔑 ÉP @timestamp → ISO-8601 STRING (ES & Kibana friendly)
        .withColumn(
            "@timestamp", date_format(col("@timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
        )
        .drop("window_start", "window_end")
        .write.format("org.elasticsearch.spark.sql")
        .mode("append")
        .options(**es_options)
        .save()
    )


q_es = (
    stateful.writeStream.outputMode("update")
    .foreachBatch(write_es)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/stateful-es")
    .start()
)

spark.streams.awaitAnyTermination()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *
from pyspark.sql.types import FloatType, StringType, StructField, StructType

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
        StructField("lat", FloatType()),
        StructField("lon", FloatType()),
    ]
)

spark = (
    SparkSession.builder.appName("WeatherCombinedPipelineDebug")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

df_json = df_raw.selectExpr("CAST(value AS STRING) AS raw_json")

df_debug_raw = df_json.writeStream.outputMode("append").format("console").start()

df_parsed = df_json.select(from_json(col("raw_json"), schema).alias("d")).select("d.*")

df_debug_parsed = df_parsed.writeStream.outputMode("append").format("console").start()

df = df_parsed.withColumn("ts", to_timestamp("timestamp")).withColumn(
    "location", struct(col("lat"), col("lon"))
)

df_debug_ts = (
    df.select("city", "timestamp", "ts", "lat", "lon", "location")
    .writeStream.outputMode("append")
    .format("console")
    .start()
)

df_wm = df.withWatermark("ts", "10 seconds")

df_window = df_wm.groupBy(
    window(col("ts"), "10 seconds"), col("city"), col("weather")
).agg(
    avg("temperature").alias("avg_temperature"), avg("humidity").alias("avg_humidity")
)

df_session = df_wm.groupBy(session_window(col("ts"), "10 seconds"), col("city")).agg(
    count("*").alias("event_count"),
    max("temperature").alias("max_temp"),
    expr("mode(weather)").alias("dom_weather"),
)

df_stateful = df_wm.groupBy("city").agg(
    avg("temperature").alias("avg_temp_running"),
    avg("humidity").alias("avg_hum_running"),
    count("*").alias("count_running"),
)

df_es_raw = df.withColumn(
    "id", sha2(concat_ws("|", "city", "ts", "temperature", "humidity", "weather"), 256)
)


def write_es(df, batch_id):
    df.write.format("org.elasticsearch.spark.sql").option(
        "es.nodes", "elasticsearch"
    ).option("es.port", "9200").option("es.nodes.wan.only", "true").option(
        "es.mapping.id", "id"
    ).option("es.resource", ES_INDEX).mode("append").save()


q_raw_es = (
    df_es_raw.writeStream.foreachBatch(write_es)
    .outputMode("append")
    .option("checkpointLocation", "/checkpoint/es_raw")
    .start()
)

q_window = df_window.writeStream.outputMode("append").format("console").start()
q_session = df_session.writeStream.outputMode("append").format("console").start()
q_stateful = df_stateful.writeStream.outputMode("update").format("console").start()

spark.streams.awaitAnyTermination()

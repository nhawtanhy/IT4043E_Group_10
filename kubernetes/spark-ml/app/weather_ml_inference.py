import os

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =====================================================
# CONFIG
# =====================================================
APP_NAME = os.getenv("APP_NAME", "weather-ml-inference")

# Inputs
SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "s3a://hust-bucket-storage/weather_silver/",
)
BRONZE_PATH = os.getenv(
    "BRONZE_PATH",
    "/checkpoint/bronze_parquet",
)

# Model
MODEL_PATH = os.getenv(
    "MODEL_PATH",
    "/models",
)

# Elasticsearch
ES_NODES = os.getenv("ES_NODES")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
ES_INDEX = os.getenv("ES_INDEX", "weather-ml-prediction")

# =====================================================
# SPARK
# =====================================================
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# 1️⃣ READ SILVER (S3 – HISTORY BACKBONE)
# =====================================================
silver = spark.read.parquet(SILVER_PATH).select(
    F.col("city"),
    F.col("timestamp").cast("timestamp"),
    F.col("temp"),
    F.col("humidity"),
    F.col("pressure"),
)

# =====================================================
# 2️⃣ READ BRONZE (REALTIME RAW)
# =====================================================
bronze = (
    spark.read.parquet(BRONZE_PATH)
    .withColumn("timestamp", F.to_timestamp(F.from_unixtime(F.col("dt"))))
    .select(
        F.col("city"),
        F.col("timestamp"),
        F.col("temp"),
        F.col("humidity"),
        F.col("pressure"),
    )
)

# =====================================================
# 3️⃣ TIME WINDOW (LAST 25 HOURS)
# =====================================================
time_filter = (
    F.col("timestamp") >= F.expr("current_timestamp() - INTERVAL 25 HOURS")
) & (F.col("timestamp") < F.expr("current_timestamp()"))

silver = silver.filter(time_filter)
bronze = bronze.filter(time_filter)

# =====================================================
# 4️⃣ UNION + DEDUPLICATE
# =====================================================
df = silver.unionByName(bronze).dropDuplicates(["city", "timestamp"])

# =====================================================
# 5️⃣ FEATURE ENGINEERING (LAG 24H)
# =====================================================
w = Window.partitionBy("city").orderBy("timestamp")

for i in range(1, 25):
    df = df.withColumn(f"temp_lag_{i}", F.lag("temp", i).over(w))

# Target (not used for inference, but required by pipeline schema)
df = df.withColumn("target_future", F.lead("temp", 1).over(w)).dropna()

# =====================================================
# 6️⃣ LOAD MODEL
# =====================================================
# =====================================================
# LOAD MODEL (ONCE – DRIVER ONLY)
# =====================================================
if not os.path.exists(MODEL_PATH):
    raise RuntimeError(f"❌ Model path not found: {MODEL_PATH}")

print(f"📦 Loading ML model from: {MODEL_PATH}")

model = PipelineModel.load(MODEL_PATH)

print("✅ Model loaded successfully")

# =====================================================
# 7️⃣ INFERENCE
# =====================================================
pred = model.transform(df)

# =====================================================
# 8️⃣ KEEP LATEST PER CITY
# =====================================================
pred = pred.withColumn("max_ts", F.max("timestamp").over(Window.partitionBy("city")))

final = pred.filter(F.col("timestamp") == F.col("max_ts")).select(
    F.col("city"),
    F.col("timestamp").alias("@timestamp"),
    F.col("prediction").alias("pred_temp_next_hour"),
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
    final.withColumn(
        "@timestamp", F.date_format(F.col("@timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
    )
    .write.format("org.elasticsearch.spark.sql")
    .mode("append")
    .options(**es_options)
    .save()
)

spark.stop()

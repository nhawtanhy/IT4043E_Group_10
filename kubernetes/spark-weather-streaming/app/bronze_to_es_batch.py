import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

APP_NAME = os.getenv("APP_NAME", "bronze-to-es-batch")

BRONZE_PATH = os.getenv("BRONZE_PATH", "/checkpoint/bronze_parquet")
STATE_PATH = os.getenv("STATE_PATH", "/checkpoint/batch_state")
STATE_FILE = "last_ts.txt"

ES_NODES = os.getenv("ES_NODES")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
ES_INDEX = os.getenv("ES_INDEX", "weather-raw")

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

# =========================
# Hadoop FS
# =========================
hadoop = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop)
bronze_path = spark._jvm.org.apache.hadoop.fs.Path(BRONZE_PATH)

# =========================
# 1. Bronze chưa tồn tại hoặc trống → exit clean
# =========================
if not fs.exists(bronze_path):
    print("Bronze path not found, exit.")
    spark.stop()
    exit(0)

files = fs.listStatus(bronze_path)
has_parquet = any(f.getPath().getName().endswith(".parquet") for f in files)

if not has_parquet:
    print("Bronze path exists but no parquet files yet, exit.")
    spark.stop()
    exit(0)

# =========================
# 2. Read Parquet SAFELY
# =========================
schema = StructType(
    [
        StructField("@timestamp", TimestampType(), True),
        StructField("city", StringType(), True),
        StructField("temp", DoubleType(), True),
    ]
)

df = spark.read.schema(schema).parquet(BRONZE_PATH)

# =========================
# 3. Read last processed timestamp
# =========================
state_dir = spark._jvm.org.apache.hadoop.fs.Path(STATE_PATH)
state_file = spark._jvm.org.apache.hadoop.fs.Path(f"{STATE_PATH}/{STATE_FILE}")

if not fs.exists(state_dir):
    fs.mkdirs(state_dir)

last_ts = None
if fs.exists(state_file):
    f = fs.open(state_file)
    last_ts = f.readUTF()
    f.close()

if last_ts:
    df = df.filter(col("@timestamp") > last_ts)

if df.rdd.isEmpty():
    print("No new data to push, exit.")
    spark.stop()
    exit(0)

# =========================
# 4. Write to Elasticsearch (BATCH)
# =========================
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
    df.write.format("org.elasticsearch.spark.sql")
    .mode("append")
    .options(**es_options)
    .save()
)

# =========================
# 5. Update marker
# =========================
new_ts = df.agg(spark_max(col("@timestamp"))).collect()[0][0]
out = fs.create(state_file, True)
out.writeUTF(str(new_ts))
out.close()

spark.stop()

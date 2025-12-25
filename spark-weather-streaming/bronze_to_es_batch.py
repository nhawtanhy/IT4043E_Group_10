import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    date_format,
    from_unixtime,
    to_timestamp,
)
from pyspark.sql.functions import max as spark_max

# =====================================================
# CONFIG
# =====================================================
APP_NAME = os.getenv("APP_NAME", "bronze-to-es-batch")

BRONZE_PATH = os.getenv("BRONZE_PATH", "/checkpoint/bronze_parquet")
STATE_PATH = os.getenv("STATE_PATH", "/checkpoint/batch_state")
STATE_FILE = "last_dt.txt"

ES_NODES = os.getenv("ES_NODES")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
ES_INDEX = os.getenv("ES_INDEX", "weather-bronze-raw")

# =====================================================
# Spark
# =====================================================
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# Hadoop FS
# =====================================================
hadoop = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop)

bronze_path = spark._jvm.org.apache.hadoop.fs.Path(BRONZE_PATH)
state_dir = spark._jvm.org.apache.hadoop.fs.Path(STATE_PATH)
state_file = spark._jvm.org.apache.hadoop.fs.Path(f"{STATE_PATH}/{STATE_FILE}")

# =====================================================
# 1️⃣ Check Bronze
# =====================================================
if not fs.exists(bronze_path):
    print("Bronze path not found, exit.")
    spark.stop()
    exit(0)

files = fs.listStatus(bronze_path)
has_parquet = any(f.getPath().getName().endswith(".parquet") for f in files)

if not has_parquet:
    print("Bronze path exists but empty, exit.")
    spark.stop()
    exit(0)

# =====================================================
# 2️⃣ Read Bronze (FULL SCHEMA – NO MANUAL SCHEMA)
# =====================================================
df = spark.read.parquet(BRONZE_PATH)

# =====================================================
# 3️⃣ Incremental filter (based on dt – event time)
# =====================================================
last_dt = None
if fs.exists(state_file):
    f = fs.open(state_file)
    last_dt = int(f.readUTF())
    f.close()

if last_dt:
    df = df.filter(col("dt") > last_dt)

if df.rdd.isEmpty():
    print("No new data to push, exit.")
    spark.stop()
    exit(0)

# =====================================================
# 4️⃣ Normalize timestamp + ES id
# =====================================================
df = (
    df
    # 🔑 REAL EVENT TIME
    .withColumn("event_time", to_timestamp(from_unixtime(col("dt"))))
    # 🔑 ES DATE FORMAT
    .withColumn("@timestamp", date_format(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss"))
    # 🔑 Idempotent ID
    .withColumn("doc_id", concat_ws("_", col("city"), col("dt")))
)

# =====================================================
# 5️⃣ Write to Elasticsearch (UPSERT)
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

(
    df.write.format("org.elasticsearch.spark.sql")
    .mode("append")
    .options(**es_options)
    .save()
)

# =====================================================
# 6️⃣ Update state (dt marker)
# =====================================================
new_dt = df.agg(spark_max(col("dt"))).collect()[0][0]

fs.mkdirs(state_dir)
out = fs.create(state_file, True)
out.writeUTF(str(new_dt))
out.close()

spark.stop()

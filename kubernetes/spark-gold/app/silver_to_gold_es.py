import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    window,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)

# =====================================================
# Config
# =====================================================
APP_NAME = os.getenv("APP_NAME", "silver-to-gold-es")

SILVER_PATH = os.getenv("SILVER_PATH", "/data/silver/weather")

STATE_PATH = os.getenv("STATE_PATH", "/checkpoint/gold_state")
STATE_FILE = "last_ts.txt"

ES_NODES = os.getenv("ES_NODES")
ES_PORT = os.getenv("ES_PORT", "9200")
ES_USER = os.getenv("ES_USER")
ES_PASS = os.getenv("ES_PASS")
ES_INDEX = os.getenv("ES_INDEX", "weather-gold-hourly-auto")

# =====================================================
# Spark Session
# =====================================================
spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# =====================================================
# Hadoop FS
# =====================================================
hadoop = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop)

silver_path = spark._jvm.org.apache.hadoop.fs.Path(SILVER_PATH)
state_dir = spark._jvm.org.apache.hadoop.fs.Path(STATE_PATH)
state_file = spark._jvm.org.apache.hadoop.fs.Path(f"{STATE_PATH}/{STATE_FILE}")

# =====================================================
# 1️⃣ Check Silver existence
# =====================================================
if not fs.exists(silver_path):
    print("Silver path not found, exit.")
    spark.stop()
    exit(0)

# =====================================================
# 2️⃣ Read Silver
# =====================================================
df = spark.read.parquet(SILVER_PATH)

# =====================================================
# 3️⃣ Load incremental state
# =====================================================
last_ts = None
if fs.exists(state_file):
    f = fs.open(state_file)
    last_ts = f.readUTF()
    f.close()

if last_ts:
    df = df.filter(col("timestamp") > last_ts)

if df.rdd.isEmpty():
    print("No new Silver data, exit.")
    spark.stop()
    exit(0)

# =====================================================
# 4️⃣ GOLD AGGREGATIONS (Hourly)
# =====================================================
gold = (
    df.groupBy(window(col("timestamp"), "1 hour"), col("city"))
    .agg(
        avg("temp").alias("avg_temp"),
        spark_min("temp").alias("min_temp"),
        spark_max("temp").alias("max_temp"),
        avg("humidity").alias("avg_humidity"),
        count("*").alias("records"),
    )
    .select(
        col("city"),
        col("window.start").alias("@timestamp"),
        col("avg_temp"),
        col("min_temp"),
        col("max_temp"),
        col("avg_humidity"),
        col("records"),
    )
)

# =====================================================
# 🔑 5️⃣ FIX QUAN TRỌNG: ÉP @timestamp → ISO STRING
# =====================================================
gold = gold.withColumn(
    "@timestamp", date_format(col("@timestamp"), "yyyy-MM-dd'T'HH:mm:ss")
)

# =====================================================
# 6️⃣ Write to Elasticsearch
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
    gold.write.format("org.elasticsearch.spark.sql")
    .mode("append")
    .options(**es_options)
    .save()
)

# =====================================================
# 7️⃣ Update state (incremental marker)
# =====================================================
new_ts = gold.select("@timestamp").agg(spark_max(col("@timestamp"))).collect()[0][0]

fs.mkdirs(state_dir)
out = fs.create(state_file, True)
out.writeUTF(str(new_ts))
out.close()

spark.stop()

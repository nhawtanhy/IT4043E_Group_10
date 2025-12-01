from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from pyspark.sql.types import *
from pyspark.sql.types import FloatType, StringType, StructField, StructType

schema = StructType(
    [
        StructField("city", StringType()),
        StructField("timestamp", StringType()),
        StructField("temperature", FloatType()),
        StructField("humidity", FloatType()),
        StructField("weather", StringType()),
    ]
)


class WeatherSession:
    def __init__(self, start=None, last=None, count=0, max_temp=None, dom_weather=None):
        self.start = start
        self.last = last
        self.count = count
        self.max_temp = max_temp
        self.dom_weather = dom_weather


def session_func(city, rows, state: GroupState):
    if state.exists:
        s = state.get()
        freq = {}
    else:
        s = WeatherSession()
        freq = {}
    for r in rows:
        ts = r.ts
        t = r.temperature
        w = r.weather
        if s.start is None:
            s.start = ts
        s.last = ts
        s.count += 1
        if s.max_temp is None or t > s.max_temp:
            s.max_temp = t
        freq[w] = freq.get(w, 0) + 1
    if len(freq) > 0:
        s.dom_weather = max(freq, key=freq.get)
    state.setTimeoutTimestamp(int(s.last.timestamp() * 1000) + 300000)
    if state.hasTimedOut:
        out = (city, s.start, s.last, s.count, s.max_temp, s.dom_weather)
        state.remove()
        return [out]
    state.update(s)
    return []


def combine_func(city, rows, state: GroupState):
    if state.exists:
        st = state.get()
    else:
        st = {"sum_temp": 0.0, "sum_hum": 0.0, "count": 0}
    for r in rows:
        st["sum_temp"] += r.temperature
        st["sum_hum"] += r.humidity
        st["count"] += 1
    avg_t = st["sum_temp"] / st["count"] if st["count"] > 0 else None
    avg_h = st["sum_hum"] / st["count"] if st["count"] > 0 else None
    state.update(st)
    return (city, avg_t, avg_h, st["count"])


spark = (
    SparkSession.builder.appName("WeatherAdvanced")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather_raw")
    .load()
)

df = (
    df_raw.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("d"))
    .select("d.*")
    .withColumn("ts", to_timestamp("timestamp"))
)

df_wm = df.withWatermark("ts", "10 minutes")

df_window = df_wm.groupBy(
    window(col("ts"), "5 minutes"), col("city"), col("weather")
).agg(
    avg("temperature").alias("avg_temperature"), avg("humidity").alias("avg_humidity")
)

df_session = (
    df.groupByKey(lambda r: r.city)
    .mapGroupsWithState(
        stateClass=WeatherSession,
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
        func=session_func,
    )
    .toDF(
        "city", "session_start", "session_end", "event_count", "max_temp", "dom_weather"
    )
)

df_stateful = (
    df.groupByKey(lambda r: r.city)
    .mapGroupsWithState(
        stateClass=dict, timeoutConf=GroupStateTimeout.NoTimeout, func=combine_func
    )
    .toDF("city", "avg_temp", "avg_humidity", "count")
)

df_es = df.withColumn(
    "id", sha2(concat_ws("|", "city", "ts", "temperature", "humidity", "weather"), 256)
)


def write_es(df, batch_id):
    df.write.format("org.elasticsearch.spark.sql").option(
        "es.nodes", "elasticsearch"
    ).option("es.port", "9200").option("es.nodes.wan.only", "true").option(
        "es.mapping.id", "id"
    ).option("es.resource", "weather-data").mode("append").save()


q1 = df_window.writeStream.outputMode("append").format("console").start()

q2 = df_session.writeStream.outputMode("append").format("console").start()

q3 = df_stateful.writeStream.outputMode("update").format("console").start()

q4 = (
    df_es.writeStream.foreachBatch(write_es)
    .option("checkpointLocation", "/checkpoint/weather_data")
    .start()
)

spark.streams.awaitAnyTermination()

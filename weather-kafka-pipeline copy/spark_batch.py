from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("WeatherBatchJob")
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

print("=== Reading parquet from HDFS ===")

df = spark.read.parquet("hdfs://namenode:8020/weather/parquet/*")

df.createOrReplaceTempView("weather")

result = spark.sql("""
    SELECT
        city,
        date(timestamp) AS date,
        AVG(temperature) AS avg_temp,
        MIN(temperature) AS min_temp,
        MAX(temperature) AS max_temp,
        AVG(humidity) AS avg_humidity
    FROM weather
    GROUP BY city, date(timestamp)
""")

print("=== Batch Aggregated Result ===")
result.show(truncate=False)
print("=== Saving batch to Elasticsearch ===")

result.write.format("org.elasticsearch.spark.sql").option(
    "es.nodes", "elasticsearch"
).option("es.port", "9200").option("es.nodes.wan.only", "true").mode("overwrite").save(
    "weather_agg"
)

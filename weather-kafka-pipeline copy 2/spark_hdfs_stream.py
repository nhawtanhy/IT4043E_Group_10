from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# 1) Schema chuẩn cho JSON từ Kafka
schema = StructType(
    [
        StructField("city", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("wind_speed", DoubleType()),
        StructField("timestamp", StringType()),  # dạng "2025-11-29 17:30:00"
    ]
)

spark = (
    SparkSession.builder.appName("WeatherToHDFS")
    .config("spark.sql.streaming.schemaInference", "false")
    .getOrCreate()
)

# 2) Đọc từ Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "weather_raw")
    .option("startingOffsets", "latest")
    .load()
)

json_df = df.selectExpr("CAST(value AS STRING) as raw_json")

# 3) Parse JSON + chuẩn hóa
parsed = json_df.select(from_json(col("raw_json"), schema).alias("data")).select(
    "data.*"
)

# 4) Partition theo ngày (YYYY-MM-DD)
partitioned = parsed.withColumn("dt", to_date(col("timestamp")))

# 5) Ghi vào HDFS dạng Parquet
query = (
    partitioned.writeStream.format("parquet")
    .option("path", "hdfs://namenode:8020/weather/parquet")
    .option("checkpointLocation", "hdfs://namenode:8020/checkpoints/weather_to_hdfs")
    .option("truncate", "false")
    .partitionBy("dt")
    .trigger(processingTime="5 minutes")
    .outputMode("append")
    .start()
)

query.awaitTermination()

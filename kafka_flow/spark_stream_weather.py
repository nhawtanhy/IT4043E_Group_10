from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import FloatType, StringType, StructType

schema = (
    StructType()
    .add("city", StringType())
    .add("timestamp", StringType())
    .add("temperature", FloatType())
    .add("humidity", FloatType())
    .add("weather", StringType())
    .add("raw_json", StringType())
)

spark = (
    SparkSession.builder.appName("KafkaWeatherStreaming")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.port", "7077")
    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")

df_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "weather_raw")
    .option("startingOffsets", "latest")
    .load()
)

df_string = df_raw.select(col("value").cast("string"))

df_parsed = df_string.select(from_json(col("value"), schema).alias("data")).select(
    "data.*"
)

query = (
    df_parsed.writeStream.outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

query.awaitTermination()

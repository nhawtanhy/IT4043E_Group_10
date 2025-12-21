import os
import sys
import time
import socket
import boto3
import zipfile
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, date_format, concat_ws, from_json
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, FloatType, IntegerType
# Machine learning Time series to predict the next hour temperature
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml import PipelineModel

# Constants
CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "hust-bucket-storage")
S3_PATHS = [f"s3a://{S3_BUCKET}/weather_data/{city}.parquet" for city in CITY_LIST]
KAFKA = "kafka:9092"
TOPIC = "weather_raw"
ES_INDEX = "weather-data"

kafka_schema = StructType([
    StructField("city", StringType()),
    StructField("timestamp", StringType()),
    StructField("description", StringType()), 
    StructField("temp", FloatType()),
    StructField("pressure", FloatType()),
    StructField("humidity", FloatType()),
    StructField("wind_speed", FloatType()),
    StructField("wind_deg", FloatType()),
    StructField("wind_gust", FloatType()),
    StructField("cloudiness", IntegerType())
])

def wait_for_elasticsearch(host="elasticsearch", port=9200, timeout=120):
    """
    Waits for Elasticsearch to become responsive before running the job.
    """
    print(f"⏳ Waiting for Elasticsearch ({host}:{port}) to come online...")
    start_time = time.time()
    
    while True:
        try:
            with socket.create_connection((host, port), timeout=3):
                print("✅ Elasticsearch is UP! Starting Spark job...")
                return True
        except (socket.timeout, ConnectionRefusedError, OSError):
            current_time = time.time()
            if current_time - start_time > timeout:
                print(f"❌ Timed out waiting for Elasticsearch after {timeout} seconds.")
                return False
            time.sleep(5)

def main():
    # Wait for ES - synchronization
    if not wait_for_elasticsearch(host="elasticsearch", port=9200):
        sys.exit(1)
        
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if not access_key or not secret_key:
        print("❌ ERROR: AWS Credentials not found in environment variables. " \
        "Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.")
        sys.exit(1)

    # Spark initialization
    spark = SparkSession.builder \
        .appName("Weather") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") # Log level: ERROR, WARN, OFF - OFF is set in production.

    # Download model (from S3), unzipping and load (pack it into a function later)
    local_dir = "/weather_model"
    local_zip_path = os.path.join(local_dir, "model.zip")
    s3 = boto3.client(
        's3',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
    s3.download_file("hust-bucket-storage", "models/vietnam_weather_gbt_v1.zip", local_zip_path)
    print(f"📦 Unzipping to {local_dir}...")
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(local_dir)
    model_path = ""
    for root, dirs, files in os.walk(local_dir):
        if "metadata" in dirs:
            model_path = root
            break
            
    if model_path != "":
        try:
            model = PipelineModel.load(model_path)
            print("Model has been successfully loaded! Inference...")
        except Exception as e:
            print(f"Can not load the model, error :{e}")
    else:
        print("No directory!")
        sys.exit(1)
    
    # Cached df if the interval is still in the same hour
    df_cached = None
    last_loaded_hour = -1

    while True:
        for i in range(len(S3_PATHS)):
            try:
                current_hour = datetime.now().hour
                if current_hour != last_loaded_hour:
                    print(f"Reading from S3, file {S3_PATHS[i]}")
                    # Read ALL of the parquet files from S3
                    df_raw = spark.read.parquet(S3_PATHS[i])

                    # Chỉnh sửa timestamp
                    if "timestamp" in df_raw.columns:
                        # Convert data if needed (legacy)
                        dtypes = dict(df_raw.dtypes)
                        if dtypes["timestamp"] in ["bigint", "long"]:
                            # Convert Nanosecond to seconds
                            df_cached = df_raw.withColumn(
                                "timestamp", 
                                date_format(
                                    (col("timestamp") / 1_000_000_000).cast(TimestampType()), 
                                    "yyyy-MM-dd HH:mm:ss"
                                )
                            )
                        else:
                            df_cached = df_raw.withColumn(
                                "timestamp", 
                                date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
                            )
                    else:
                        df_cached = df_raw

                    df_cached = df_cached.filter(
                        (col("timestamp") >= expr("current_timestamp() - INTERVAL 25 HOURS")) &
                        (col("timestamp") < expr("current_timestamp()"))
                    )
                # Read kafka stream
                kafka_data_raw = (
                    spark.read.format("kafka") \
                    .option("kafka.bootstrap.servers", KAFKA) \
                    .option("subscribe", TOPIC) \
                    .option("startingOffsets", "earliest") \
                    .option("endingOffsets", "latest") \
                    .load()
                )

                kafka_data = kafka_data_raw.selectExpr("CAST(value AS STRING) AS json")
                df_kafka = kafka_data.select(from_json(col("json"), kafka_schema).alias("data")).select("data.*")
                df_kafka = df_kafka.withColumn(
                    "timestamp",
                    date_format(F.to_timestamp(col("timestamp")), "yyyy-MM-dd HH:mm:ss")
                )
                df_combined = df_cached.unionByName(df_kafka, allowMissingColumns=True)
                df_combined.show(25, truncate=False)
                # ML to predict the next hours temperature using the lags of previous 24 hours

                # Preprocess data for ML
                df_combined = df_combined.drop('feels_like','temp_min','temp_max','visibility','wind_gust')
                windowSpec = Window.partitionBy("city").orderBy("timestamp")
                lag_cols = []
                for i in range(1, 25):
                    col_name = f"temp_lag_{i}"
                    df_combined = df_combined.withColumn(col_name, F.lag("temp", i).over(windowSpec))
                    lag_cols.append(col_name)
                df_features = df_combined.withColumn("target_future", F.lead("temp", 1).over(windowSpec))
                # Clean rows with missing data (temp_lag_1 of the first row = null for example, since there is no previous data)
                df_clean = df_features.dropna()
                # Rank to divide train-test
                print("🧠 Running Global Inference...")
                predictions = model.transform(df_clean)

                # VIBE CODE ALERT !!!!!
                predictions = predictions.withColumn("max_ts", F.max("timestamp").over(Window.partitionBy("city")))
                
                # Filter: Keep ONLY the row where timestamp == max_ts
                latest_row = predictions.filter(col("timestamp") == col("max_ts"))

                # B. Move Prediction to target_future
                # We overwrite 'target_future' with the value from 'prediction'
                final_result = latest_row.withColumn("target_future", col("prediction"))

                # C. Cleanup Columns
                # 1. Identify lag columns dynamically
                cols_to_drop = [c for c in final_result.columns if "temp_lag_" in c]
                # 2. Add other model-specific columns to drop list
                cols_to_drop += ["features", "prediction", "max_ts", "rank"]
                
                # Perform the drop
                final_result = final_result.drop(*cols_to_drop)

                if final_result.count() > 0:
                    # Write to Elasticsearch
                    ES_INDEX = "weather-data"
                    final_result.show(truncate=False)
                    # df_final = df_filtered.withColumn("es_id", concat_ws("_", col("city"), col("timestamp")))
                    print("   🚀 Writing to Elasticsearch...")
                    final_result.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("es.nodes", "elasticsearch") \
                    .option("es.port", "9200") \
                    .option("es.resource", f"{ES_INDEX}") \
                    .option("es.nodes.wan.only", "true") \
                    .mode("append") \
                    .save()
                    
                else:
                    print("No data found in the 60-day window.")

            except Exception as e:
                print("------------------------------SPARK FAILED!------------------------------")
                print(f"Error: {e}")
                print("-------------------------------------------------------------------------------")
                time.sleep(60)
                spark.stop()
                sys.exit(1)

if __name__ == "__main__":
    main()
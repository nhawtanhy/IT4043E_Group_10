import os
import sys
import time
import socket
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, date_format, concat_ws
from pyspark.sql.types import TimestampType

# Constants
CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "hust-bucket-storage")
S3_PATHS = [f"s3a://{S3_BUCKET}/weather_data/{city}.parquet" for city in CITY_LIST]

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

    if not wait_for_elasticsearch(host="elasticsearch", port=9200):
        sys.exit(1)
        
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    if not access_key or not secret_key:
        print("❌ ERROR: AWS Credentials not found in environment variables. " \
        "Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.")
        sys.exit(1)

    print("-------------------------------------------------------------------------------")

    # 1. Initialize Spark Session with S3 Support
    spark = SparkSession.builder \
        .appName("WeatherHistoryBatch") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        .getOrCreate()

    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("ERROR")
    for i in range(len(S3_PATHS)):
        print(f"Reading from S3, file {S3_PATHS[i]}")
        try:
            # Read Parquet file
            df_raw = spark.read.parquet(S3_PATHS[i])

            if "timestamp" in df_raw.columns:
                # Check if it needs conversion from Long/BigInt
                dtypes = dict(df_raw.dtypes)
                if dtypes["timestamp"] in ["bigint", "long"]:
                    # Convert Nanosecond to seconds
                    df = df_raw.withColumn(
                        "timestamp", 
                        date_format(
                            (col("timestamp") / 1_000_000_000).cast(TimestampType()), 
                            "yyyy-MM-dd HH:mm:ss"
                        )
                    )
                else:
                    # If it's already a timestamp object, just format it
                    df = df_raw.withColumn(
                        "timestamp", 
                        date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
                    )
            else:
                df = df_raw

            print(f"   ✅ Data loaded. Total Raw Count: {df.count()}")

            # 3. Filter for Last 60 Days
            df_filtered = df.filter(
                col("timestamp") >= expr("date_sub(current_timestamp(), 60)")
            )
            filtered_count = df_filtered.count()

            if filtered_count > 0:

                # Write to Elasticsearch
                ES_INDEX = "weather-data"
                df_final = df_filtered.withColumn("es_id", concat_ws("_", col("city"), col("timestamp")))
                print("   🚀 Writing to Elasticsearch...")
                df_filtered.write \
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
            print("------------------------------SPARK-BATCH FAILED!------------------------------")
            print(f"Error: {e}")
            print("-------------------------------------------------------------------------------")
            time.sleep(300)
            sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()
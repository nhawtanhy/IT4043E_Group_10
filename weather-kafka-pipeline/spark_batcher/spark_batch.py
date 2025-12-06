import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count, current_timestamp, expr
from pyspark.sql.types import TimestampType

# Constants
CITY_LIST = ["Hanoi", "Ho Chi Minh City", "Da Nang", "Haiphong", "Can Tho"]
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "hust-bucket-storage")
S3_PATHS = []
for CITY in CITY_LIST:
    S3_PATHS.append("s3a://"+ str(S3_BUCKET) + "/weather_data/" + str(CITY) + ".parquet")

def main():
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
    spark.sparkContext.setLogLevel("WARN")

    print(f"Reading from S3, file ")
    try:
        # 2. Read all Parquet files
        df_raw = spark.read.parquet(S3_PATH)
        
        # --- FIX 3: Convert the raw Long (Nanosecond) to Timestamp ---
        # Because we used 'nanosAsLong', the 'timestamp' column is now a big Integer.
        # We divide by 1,000,000 to get Milliseconds, then cast to Timestamp.
        if "timestamp" in df_raw.columns:
            # Check if it needs conversion (if it's not already a timestamp)
            if dict(df_raw.dtypes)["timestamp"] == "bigint" or dict(df_raw.dtypes)["timestamp"] == "long":
                 df = df_raw.withColumn("timestamp", (col("timestamp") / 1000000).cast(TimestampType()))
            else:
                df = df_raw
        else:
            df = df_raw

        print(f"   ✅ Data loaded. Total Raw Count: {df.count()}")

        # 3. Filter for Last 60 Days
        df_filtered = df.filter(
            col("timestamp") >= expr("date_sub(current_timestamp(), 60)")
        )
        filtered_count = df_filtered.count()

        if filtered_count > 0:
            # 4. Perform Analytics: Average Temp & Humidity per City
            print("\n📊 Average Weather Conditions (Last 2 Months):")
            
            stats_df = df_filtered.groupBy("city").agg(
                count("*").alias("record_count"),
                avg("temp").alias("avg_temp"),
                min("temp").alias("min_temp"),
                max("temp").alias("max_temp"),
                avg("humidity").alias("avg_humidity")
            ).orderBy("city")
            
            stats_df.show(truncate=False)
            
            # --- OPTIONAL: Write to Elasticsearch (Based on your requirements) ---
            # To enable this, uncomment below:
            ES_INDEX = "weather-data"

            print("   🚀 Writing to Elasticsearch...")
            stats_df.write \
               .format("org.elasticsearch.spark.sql") \
               .option("es.nodes", "elasticsearch") \
               .option("es.port", "9200") \
               .option("es.resource", f"{ES_INDEX}") \
               .option("es.nodes.wan.only", "true") \
               .save()
            
        else:
            print("   ⚠️ No data found in the 60-day window.")

    except Exception as e:
        print("------------------------------SPARK-BATCH FAILED!------------------------------")
        print(f"Error: {e}")
        print("-------------------------------------------------------------------------------")
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()
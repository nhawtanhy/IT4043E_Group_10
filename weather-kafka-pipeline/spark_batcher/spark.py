import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, count, current_timestamp, expr

# Constants
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "hust-bucket-storage")
S3_PATH = f"s3a://{S3_BUCKET}/weather_data/"

def main():
    print(f"🚀 Starting Spark Batch Job...")
    print(f"   Reading from: {S3_PATH}")

    # 1. Initialize Spark Session with S3 Support
    # Note: We configure the S3A file system to use AWS credentials from Env Vars
    spark = SparkSession.builder \
        .appName("WeatherHistoryBatch") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # Set log level to WARN to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    try:
        # 2. Read all Parquet files from the folder
        # Spark treats the folder as a dataset and merges all city files automatically
        df = spark.read.parquet(S3_PATH)
        
        print(f"   ✅ Data loaded. Total Raw Count: {df.count()}")

        # 3. Filter for Last 60 Days
        # If data is less than 60 days, this simply takes everything available
        df_filtered = df.filter(
            col("timestamp") >= expr("date_sub(current_timestamp(), 60)")
        )
        
        filtered_count = df_filtered.count()
        print(f"   📅 Data from last 60 days: {filtered_count} rows")

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
            
            # Optional: Save the processed results back to S3
            # output_path = f"s3a://{S3_BUCKET}/processed_stats/weather_report_60d"
            # stats_df.write.mode("overwrite").parquet(output_path)
            # print(f"   💾 Stats saved to {output_path}")

        else:
            print("   ⚠️ No data found in the 60-day window.")

    except Exception as e:
        print(f"   ❌ Spark Job Failed: {e}")
        # Common error: S3 keys missing or path does not exist yet
        sys.exit(1)

    spark.stop()

if __name__ == "__main__":
    main()
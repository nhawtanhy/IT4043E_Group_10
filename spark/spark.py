import os
import json
import requests
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, lit, current_date
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    FloatType, TimestampType
)

# --- Configuration ---
BUCKET_NAME = "hust-bucket-storage"
S3_PATH = f"s3a://{BUCKET_NAME}/weather_data"

# FIX 3: Fail fast if API key isn't set. No hardcoded fallback.
API_KEY = os.getenv("OPENWEATHER_API_KEY","4059a105896914f6ca10742d4ab0c123")
if not API_KEY:
    raise ValueError("ERROR: OPENWEATHER_API_KEY environment variable is not set.")

CITY_LIST = [
    "Hanoi", 
    "Ho Chi Minh City", 
    "Da Nang", 
    "Haiphong", 
    "Can Tho"
]

# FIX 4: Define an explicit schema for the data
# This is more robust and efficient than schema inference
WEATHER_SCHEMA = StructType([
    StructField("city", StringType()),
    StructField("timestamp", StringType()), # Using StringType for ISO format
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("weather", StringType()),
    StructField("raw_json", StringType())
])

# --- Spark setup ---
# Your Spark configuration is good for S3 access
spark = (
    SparkSession.builder
    .appName("ParallelWeatherDataToS3")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.300")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

# --- UDF Definition ---
def _fetch_weather_logic(city, api_key):
    """
    Internal function to fetch weather.
    This will be wrapped by a Spark UDF.
    """
    # FIX 2: All imports must be *inside* the UDF
    # so they are available on the worker nodes.
    import requests
    import json
    from datetime import datetime

    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    
    try:
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            temp = float(data["main"]["temp"])
            hum = float(data["main"]["humidity"])
            
            # Return data matching the schema
            return {
                "city": city,
                "timestamp": datetime.now().isoformat(),
                "temperature": temp,
                "humidity": hum,
                "weather": data["weather"][0]["description"],
                "raw_json": json.dumps(data)
            }
        else:
            print(f"[{datetime.now()}] Failed to fetch data for {city}: {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] Request failed for {city}: {e}")
        return None
    except (KeyError, TypeError) as e:
        print(f"[{datetime.now()}] Failed to parse data for {city}: {e}")
        return None

# Wrap the Python function in a Spark UDF, applying the schema
fetch_weather_udf = udf(_fetch_weather_logic, returnType=WEATHER_SCHEMA)


def main():
    print(f"[{datetime.now()}] Starting parallel weather data collection...")

    # FIX 1: Create a DataFrame of cities *first*
    # This is the "E" (Extract) part of the parallel job
    city_df = spark.createDataFrame([(city,) for city in CITY_LIST], ["city"])
    
    # Add the API key as a literal column so the UDF can access it
    city_df_with_key = city_df.withColumn("api_key", lit(API_KEY))

    # Run the UDF in parallel across all workers/partitions
    # This is the "T" (Transform) part
    raw_data_df = city_df_with_key.withColumn(
        "data", 
        fetch_weather_udf(col("city"), col("api_key"))
    )

    # Filter out any failed API calls and flatten the struct
    final_df = raw_data_df \
        .filter(col("data").isNotNull()) \
        .select("data.*") # Flattens the struct into columns

    # Check if any data was collected
    if final_df.rdd.isEmpty():
        print("No data collected — exiting.")
        spark.stop()
        return

    print("--- Sample of collected data ---")
    final_df.show(5, truncate=False)

    # FIX 5: Append data and partition by date for organization
    print(f"[{datetime.now()}] Uploading to S3 bucket {BUCKET_NAME}...")
    
    (
        final_df
        .withColumn("collection_date", current_date()) # Add a date column for partitioning
        .write
        .mode("append")  # Append new data, don't overwrite history
        .partitionBy("collection_date") # Store in folders like /collection_date=2025-11-12/
        .json(S3_PATH) # Write as JSON
    )

    print(f"[{datetime.now()}] Successfully uploaded to {S3_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()
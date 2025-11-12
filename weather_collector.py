import requests
import os
import boto3
import botocore
from datetime import datetime
import constant as const
import json

BUCKET_NAME = 'hust-bucket-storage'
API_KEY = os.getenv("OPENWEATHER_API_KEY", const.OPENWEATHER_API_KEY) # Change this to your API key

CITY_LIST = [
    "Hanoi", 
    "Ho Chi Minh City", 
    "Da Nang", 
    "Haiphong", 
    "Can Tho"
] # List of cities to collect weather data so far
URL_LIST = [
    f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    for city in CITY_LIST
]

def main():
    try:
        s3_client = boto3.client('s3')
    except botocore.exceptions.NoCredentialsError:
        print("Credentials not found. Please configure AWS credentials.")
        print("You can do this by running: aws configure")
        exit()
    for i in range(len(CITY_LIST)):
        print(f"[{datetime.now()}] Collecting weather data for {CITY_LIST[i]}...")
        response = requests.get(URL_LIST[i])
        if response.status_code == 200:
            data = response.json()
            city_name = CITY_LIST[i].replace(" ", "_").lower()
            object_key = f"weather_data/{city_name}.json"
            
            # 6. Convert the Python dictionary (data) to a JSON formatted string
            json_data_string = json.dumps(data, indent=2)

            try:
                s3_client.put_object(
                    Bucket=BUCKET_NAME,
                    Key=object_key,
                    Body=json_data_string,
                    ContentType='application/json' # Good practice to set the content type
                )
                print(f"[{datetime.now()}] Successfully uploaded to s3://{BUCKET_NAME}/{object_key}")
            
            except Exception as e:
                print(f"[{datetime.now()}] ERROR: Failed to upload to S3. {e}")

            print(f"[{datetime.now()}] Uploading data for {city_name} to S3 bucket {BUCKET_NAME}...")
            print(f"Temperature: {data['main']['temp']}°C")
            print(f"Humidity: {data['main']['humidity']}%")
            print(f"Weather: {data['weather'][0]['description']}")
        else:
            print("Failed to fetch data:", response.text)
    

if __name__ == "__main__":
    main()

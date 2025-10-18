import boto3
import botocore

# --- Configuration ---
LOCAL_FILE_PATH = 'testUpload.txt'  # The file you want to upload
BUCKET_NAME = 'hust-bucket-storage'             # Your target S3 bucket
S3_OBJECT_KEY = 'testing/nameOfTheFileOnAWS'    # The desired name/path in S3

# ---------------------

# 1. Create an S3 client
# Boto3 will automatically look for credentials in your environment
# (e.g., from 'aws configure' or environment variables)
try:
    s3_client = boto3.client('s3')
except botocore.exceptions.NoCredentialsError:
    print("Credentials not found. Please configure AWS credentials.")
    print("You can do this by running: aws configure")
    exit()

# 2. Upload the file
print(f"Uploading '{LOCAL_FILE_PATH}' to '{BUCKET_NAME}/{S3_OBJECT_KEY}'...")
try:
    s3_client.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, S3_OBJECT_KEY)
    print("✅ Upload Successful")

except FileNotFoundError:
    print(f"Error: The file '{LOCAL_FILE_PATH}' was not found.")
except botocore.exceptions.NoCredentialsError:
    print("Error: Credentials not valid or found.")
except botocore.exceptions.ClientError as e:
    # Handle specific S3 errors, e.g., 'AccessDenied'
    error_code = e.response.get('Error', {}).get('Code')
    if error_code == 'AccessDenied':
        print("Error: Access Denied. Check your IAM permissions.")
    elif error_code == 'NoSuchBucket':
        print(f"Error: Bucket '{BUCKET_NAME}' does not exist.")
    else:
        print(f"An S3 client error occurred: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
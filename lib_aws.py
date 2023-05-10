import boto3
import os
from dotenv import load_dotenv


def create_session():

    # Load env
    load_dotenv(".env")
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_scret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    
    # Authenticate S3
    session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_scret_access_key)
    s3 = session.resource('s3')
    print("Authenticated")

    return s3


def upload_to_aws(session, bucket, file):

    # Upload to S3
    session.Bucket(bucket).upload_file(file, file)
    print(f"Uploaded to s3://{bucket}/{file}")
    
def download_from_aws(session, bucket, file):

    # Download AWS
    session.Bucket(bucket).download_file(file, file)
    print(f"Downloaded from s3://{bucket}/{file}")

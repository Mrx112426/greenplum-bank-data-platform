import os
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError


endpoint_url = os.environ["PLATFORM_MINIO_ENDPOINT"]
access_key = os.environ["PLATFORM_MINIO_ACCESS_KEY"]
secret_key = os.environ["PLATFORM_MINIO_SECRET_KEY"]
bucket_name = os.environ["PLATFORM_MINIO_BUCKET"]

client = boto3.client(
    "s3",
    endpoint_url=endpoint_url,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)

for attempt in range(1, 31):
    try:
        client.head_bucket(Bucket=bucket_name)
        print(f"MinIO bucket '{bucket_name}' already exists")
        break
    except ClientError as error:
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 404:
            client.create_bucket(Bucket=bucket_name)
            print(f"Created MinIO bucket '{bucket_name}'")
            break
        if status not in {400, 403, 404, 500, 503}:
            raise
    except EndpointConnectionError:
        pass

    if attempt == 30:
        raise RuntimeError("MinIO did not become ready within 60 seconds")

    print(f"Waiting for MinIO ({attempt}/30)")
    time.sleep(2)

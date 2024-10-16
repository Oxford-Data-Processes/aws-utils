import boto3
import csv
import os
from io import StringIO


class S3Client:
    def __init__(
        self, aws_access_key_id, aws_secret_access_key, aws_session_token, aws_region
    ):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=aws_region,
        )

    def load_csv_from_s3(self, bucket_name, csv_key):
        csv_object = self.s3_client.get_object(Bucket=bucket_name, Key=csv_key)
        csv_data = csv_object["Body"].read().decode("utf-8")
        csv_reader = csv.reader(StringIO(csv_data))
        data = [row for row in csv_reader]
        return data

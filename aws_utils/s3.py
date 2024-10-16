import boto3
import csv
import os
import json
from io import StringIO


class S3Handler:
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

    def load_json_from_s3(self, bucket_name, json_key):
        json_object = self.s3_client.get_object(Bucket=bucket_name, Key=json_key)
        json_data = json_object["Body"].read()
        return json.loads(json_data)

    def load_parquet_from_s3(self, bucket_name, parquet_key):
        parquet_object = self.s3_client.get_object(Bucket=bucket_name, Key=parquet_key)
        parquet_data = parquet_object["Body"].read()
        return parquet_data

    def upload_parquet_to_s3(self, bucket_name, parquet_key, parquet_data):
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=parquet_key,
            Body=parquet_data,
            ContentType="application/octet-stream",
        )

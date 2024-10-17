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

    def load_excel_from_s3(self, bucket_name, object_key):
        response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        excel_data = response["Body"].read()
        return excel_data

    def upload_parquet_to_s3(self, bucket_name, parquet_key, parquet_data):
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=parquet_key,
            Body=parquet_data,
            ContentType="application/octet-stream",
        )

    def upload_excel_to_s3(self, bucket_name, excel_key, excel_data):
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=excel_key,
            Body=excel_data,
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def upload_json_to_s3(self, bucket_name, json_key, json_data):
        json_data = json.dumps(json_data)
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=json_key,
            Body=json_data,
            ContentType="application/json",
        )

    def list_objects(self, bucket_name, prefix):
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return response.get("Contents", [])

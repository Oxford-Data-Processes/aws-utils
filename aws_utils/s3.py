import boto3
import csv
import os
import json
from io import StringIO


class S3Utils:
    @staticmethod
    def extract_partition_values(object_key: str) -> tuple[dict, list, str]:
        """
        Extract partition values from an S3 object key.

        Args:
            object_key (str): The S3 object key to extract partition values from.

        Returns:
            tuple: A tuple containing:
                - dict: A dictionary of partition values. Eg. {'year': '2024', 'month': '01', 'day': '01'}
                - list: A list of paths extracted from the object key. Eg. ['path', 'to', 'folder']
                - str: The file name extracted from the object key. Eg. 'file.csv'
        """
        partition_values = {}
        parts = object_key.split("/")
        paths = []

        for part in parts:
            if "%" in part:
                part = part.replace("%3D", "=")
                if "=" in part:
                    key, value = part.split("=")
                    partition_values[key] = value
            elif "." not in part:
                paths.append(part)
            else:
                file_name = part

        return partition_values, paths, file_name


class S3Handler:
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN"),
            region_name=os.environ.get("AWS_REGION"),
        )

    def load_csv_from_s3(self, bucket_name: str, csv_key: str) -> list:
        """
        Load a CSV file from S3.

        Args:
            bucket_name (str): The name of the S3 bucket.
            csv_key (str): The key of the CSV file in S3.

        Returns:
            list: A list of rows from the CSV file, first row is the header.
        """
        csv_object = self.s3_client.get_object(Bucket=bucket_name, Key=csv_key)
        csv_data = csv_object["Body"].read().decode("utf-8")
        csv_reader = csv.reader(StringIO(csv_data))
        data = [row for row in csv_reader]
        return data

    def load_json_from_s3(self, bucket_name: str, json_key: str) -> dict:
        """
        Load a JSON file from S3.

        Args:
            bucket_name (str): The name of the S3 bucket.
            json_key (str): The key of the JSON file in S3.

        Returns:
            dict: The JSON data as a dictionary.
        """
        json_object = self.s3_client.get_object(Bucket=bucket_name, Key=json_key)
        json_data = json_object["Body"].read()
        return json.loads(json_data)

    def load_parquet_from_s3(self, bucket_name: str, parquet_key: str) -> bytes:
        """
        Load a Parquet file from S3.

        Args:
            bucket_name (str): The name of the S3 bucket.
            parquet_key (str): The key of the Parquet file in S3.

        Returns:
            bytes: The raw Parquet data.
        """
        parquet_object = self.s3_client.get_object(Bucket=bucket_name, Key=parquet_key)
        parquet_data = parquet_object["Body"].read()
        return parquet_data

    def load_excel_from_s3(self, bucket_name: str, object_key: str) -> bytes:
        """
        Load an Excel file from S3.

        Args:
            bucket_name (str): The name of the S3 bucket.
            object_key (str): The key of the Excel file in S3.

        Returns:
            bytes: The raw Excel data.
        """
        response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        excel_data = response["Body"].read()
        return excel_data

    def upload_parquet_to_s3(
        self, bucket_name: str, parquet_key: str, parquet_data: bytes
    ):
        """
        Upload a Parquet file to S3.

        Args:
            bucket_name (str): The name of the S3 bucket.
            parquet_key (str): The key for the Parquet file in S3.
            parquet_data (bytes): The raw Parquet data to upload.
        """
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=parquet_key,
            Body=parquet_data,
            ContentType="application/octet-stream",
        )

    def upload_excel_to_s3(self, bucket_name: str, excel_key: str, excel_data: bytes):
        """
        Upload an Excel file to S3.

        Args:
            bucket_name (str): The name of the S3 bucket.
            excel_key (str): The key for the Excel file in S3.
            excel_data (bytes): The raw Excel data to upload.
        """
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=excel_key,
            Body=excel_data,
            ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def upload_json_to_s3(self, bucket_name: str, json_key: str, json_data: dict):
        """
        Upload a JSON file to S3.

        Args:
            bucket_name (str): The name of the S3 bucket.
            json_key (str): The key for the JSON file in S3.
            json_data (dict): The JSON data to upload.
        """
        json_data = json.dumps(json_data)
        self.s3_client.put_object(
            Bucket=bucket_name,
            Key=json_key,
            Body=json_data,
            ContentType="application/json",
        )

    def list_objects(self, bucket_name: str, prefix: str) -> list:
        """
        List objects in an S3 bucket with a specific prefix.

        Args:
            bucket_name (str): The name of the S3 bucket.
            prefix (str): The prefix to filter the objects.

        Returns:
            list: A list of objects in the specified bucket with the given prefix.
        """
        response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        return response.get("Contents", [])

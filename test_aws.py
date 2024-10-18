from aws_utils import iam
import streamlit as st
import os
import boto3
from datetime import datetime


class GlueHandler:
    def __init__(self):
        self.glue_client = boto3.client("glue", region_name=os.environ["AWS_REGION"])

    def build_partition_location(
        self, bucket_name: str, table_name: str, partition_values: dict
    ) -> str:
        partition_path = "/".join(
            f"{key}={value}" for key, value in partition_values.items()
        )
        return f"s3://{bucket_name}/{table_name}/{partition_path}/"

    def add_partition_to_glue(
        self,
        database_name: str,
        table_name: str,
        bucket_name: str,
        partition_values: dict,
    ):
        current_time = datetime.now().isoformat() + "Z"

        response = self.glue_client.get_table(
            DatabaseName=database_name,
            Name=table_name,
            CatalogId=os.environ["AWS_ACCOUNT_ID"],
        )
        paths = [
            column["Name"]
            for column in response["Table"]["StorageDescriptor"]["Columns"]
        ]

        partition_location = self.build_partition_location(
            bucket_name, table_name, partition_values
        )

        partition_input = {
            "Values": list(partition_values.values()),
            "LastAccessTime": current_time,
            "StorageDescriptor": {
                "Columns": [],
                "Location": partition_location,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "Compressed": True,
                "NumberOfBuckets": -1,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"paths": ",".join(paths)},
                },
                "BucketColumns": [],
                "SortColumns": [],
                "Parameters": {
                    "sizeKey": "19420",
                    "objectCount": "1",
                    "recordCount": "1",
                    "averageRecordSize": "19420",
                    "compressionType": "SNAPPY",
                    "classification": "parquet",
                    "typeOfData": "file",
                },
                "StoredAsSubDirectories": False,
            },
            "Parameters": {},
        }

        try:
            self.glue_client.create_partition(
                DatabaseName=database_name,
                TableName=table_name,
                PartitionInput=partition_input,
            )
        except Exception as e:
            print(f"Error creating partition: {e}")
            raise


bucket_name = "greenmotion-bucket-654654324108"
database_name = "do_you_spain"
table_name = "processed"
partition_values = {
    "year": "2025",
    "month": "11",
    "day": "17",
    "hour": "08",
    "rental_period": "02",
}

os.environ["AWS_ACCESS_KEY_ID_ADMIN"] = st.secrets["aws_credentials"][
    "AWS_ACCESS_KEY_ID"
]
os.environ["AWS_SECRET_ACCESS_KEY_ADMIN"] = st.secrets["aws_credentials"][
    "AWS_SECRET_ACCESS_KEY"
]

iam_instance = iam.IAM(stage=os.environ["STAGE"])
iam.AWSCredentials.get_aws_credentials(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID_ADMIN"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY_ADMIN"],
    iam_instance=iam_instance,
)

glue_client = boto3.client("glue", region_name=os.environ["AWS_REGION"])
partitions_response = glue_client.get_partitions(
    DatabaseName=database_name, TableName=table_name
)

partitions = partitions_response.get("Partitions", [])

new_partition = tuple(partition_values.values())
existing_partitions = {tuple(p["Values"]) for p in partitions}

print(existing_partitions)

if new_partition in existing_partitions:
    print(f"Partition with values {partition_values} already exists.")
else:
    print(f"Partition with values {partition_values} does not exist.")
    glue_handler = GlueHandler()
    glue_handler.add_partition_to_glue(
        database_name, table_name, bucket_name, partition_values
    )
    print(f"Partition added for values: {partition_values}")

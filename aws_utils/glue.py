import boto3
from datetime import datetime
import os


class GlueHandler:
    def __init__(self):
        self.glue_client = boto3.client("glue", region_name=os.environ["AWS_REGION"])

    def build_partition_location(self, bucket_name, table_name, partition_values):
        partition_path = "/".join(
            f"{key}={value}" for key, value in partition_values.items()
        )
        return f"s3://{bucket_name}/{table_name}/{partition_path}/"

    def add_partition_to_glue(
        self, database_name, table_name, bucket_name, partition_values
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
            "Values": partition_values,
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
            raise e

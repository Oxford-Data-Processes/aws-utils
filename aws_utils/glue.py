import boto3
from datetime import datetime
import os
from typing import Dict, Any, List


class GlueHandler:
    def __init__(self) -> None:
        """
        Initializes the GlueHandler by creating a Glue client using AWS credentials
        from environment variables.
        """
        self.glue_client = boto3.client("glue", region_name=os.environ["AWS_REGION"])

    def build_partition_location(
        self,
        bucket_name: str,
        database_name: str,
        table_name: str,
        partition_values: Dict[str, Any],
    ) -> str:
        """
        Constructs the S3 location for a partition based on the provided bucket,
        database, table, and partition values.

        Args:
            bucket_name (str): The name of the S3 bucket.
            database_name (str): The name of the Glue database.
            table_name (str): The name of the Glue table.
            partition_values (Dict[str, Any]): A dictionary of partition values.

        Returns:
            str: The S3 location for the partition.
        """
        partition_path = "/".join(
            f"{key}={value}" for key, value in partition_values.items()
        )
        return f"s3://{bucket_name}/{database_name}/{table_name}/{partition_path}/"

    def add_partition_to_glue(
        self,
        database_name: str,
        table_name: str,
        bucket_name: str,
        partition_values: Dict[str, Any],
    ) -> None:
        """
        Adds a new partition to a Glue table.

        Args:
            database_name (str): The name of the Glue database.
            table_name (str): The name of the Glue table.
            bucket_name (str): The name of the S3 bucket.
            partition_values (Dict[str, Any]): A dictionary of partition values.

        Raises:
            Exception: If there is an error while creating the partition.
        """
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
            bucket_name, database_name, table_name, partition_values
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
            raise e

    def get_all_databases(self) -> List[str]:
        """
        Retrieves a list of all databases in the Glue catalog.

        Returns:
            List[str]: A list of database names.
        """
        databases = []
        paginator = self.glue_client.get_paginator("get_databases")
        for page in paginator.paginate(CatalogId=os.environ["AWS_ACCOUNT_ID"]):
            databases.extend([db["Name"] for db in page["DatabaseList"]])
        return databases

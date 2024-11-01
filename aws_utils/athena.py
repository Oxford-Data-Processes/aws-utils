import boto3
import os
import time
from typing import Dict


class AthenaHandler:
    def __init__(self, database: str, workgroup: str, output_bucket: str):
        self.database = database
        self.workgroup = workgroup
        self.output_bucket = output_bucket
        self.athena_client = boto3.client(
            "athena", region_name=os.environ["AWS_REGION"]
        )

    def run_query_and_get_results(self, query: str) -> list[Dict[str, str]]:
        query_id = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": f"s3://{self.output_bucket}/"},
            WorkGroup=self.workgroup,
        )["QueryExecutionId"]

        # Wait for the query to complete
        while True:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_id)
            status = response["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(1)

        if status == "SUCCEEDED":
            results = self.athena_client.get_query_results(QueryExecutionId=query_id)
            # Extract the column names
            columns = [
                col["Name"]
                for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            ]
            # Extract the rows
            rows = results["ResultSet"]["Rows"][1:]  # Skip the header row
            return [
                {
                    columns[i]: row["Data"][i].get("VarCharValue", "")
                    for i in range(len(columns))
                }
                for row in rows
            ]
        else:
            raise Exception(f"Query failed with status: {status}")

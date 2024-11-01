import boto3
import os
import time
from typing import Dict


class AthenaHandler:
    def __init__(self, database: str, workgroup: str, output_bucket: str):
        """
        Initializes the AthenaHandler with the specified database, workgroup, and output bucket.

        Args:
            database (str): The name of the Athena database to run queries against.
            workgroup (str): The name of the Athena workgroup to use for query execution.
            output_bucket (str): The S3 bucket where query results will be stored.
        """
        self.database = database
        self.workgroup = workgroup
        self.output_bucket = output_bucket
        self.athena_client = boto3.client(
            "athena", region_name=os.environ["AWS_REGION"]
        )

    def run_query_and_get_results(self, query: str) -> list[Dict[str, str]]:
        """
        Runs a query against the specified Athena database and retrieves the results.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            list[Dict[str, str]]: A list of dictionaries representing the query results,
            where each dictionary corresponds to a row and the keys are column names.

        Raises:
            Exception: If the query execution fails or is cancelled.
        """
        query_id = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={"OutputLocation": f"s3://{self.output_bucket}/"},
            WorkGroup=self.workgroup,
        )["QueryExecutionId"]

        while True:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_id)
            status = response["QueryExecution"]["Status"]["State"]
            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(1)

        if status == "SUCCEEDED":
            results = self.athena_client.get_query_results(QueryExecutionId=query_id)
            columns = [
                col["Name"]
                for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
            ]
            rows = results["ResultSet"]["Rows"][1:]
            return [
                {
                    columns[i]: row["Data"][i].get("VarCharValue", "")
                    for i in range(len(columns))
                }
                for row in rows
            ]
        else:
            raise Exception(f"Query failed with status: {status}")

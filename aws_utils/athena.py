import boto3
import os
import time


class AthenaQueryExecutor:
    def __init__(self, database: str, workgroup: str, output_bucket: str):
        self.database = database
        self.workgroup = workgroup
        self.output_bucket = output_bucket
        self.athena_client = boto3.client(
            "athena", region_name=os.environ["AWS_REGION"]
        )

    def run_query(self, query: str) -> list:
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": self.database},
            ResultConfiguration={
                "OutputLocation": f"s3://{self.output_bucket}/athena-results/"
            },
            WorkGroup=self.workgroup,
        )

        query_execution_id = response["QueryExecutionId"]
        return self._get_query_results(query_execution_id)

    def _get_query_results(self, query_execution_id: str) -> list:
        results = []
        next_token = None

        while True:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            query_state = response["QueryExecution"]["Status"]["State"]

            if query_state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(0.5)

        if query_state == "SUCCEEDED":
            while True:
                params = {"QueryExecutionId": query_execution_id}
                if next_token:
                    params["NextToken"] = next_token

                response = self.athena_client.get_query_results(**params)
                columns = [
                    col.get("VarCharValue", None)  # Use get to avoid KeyError
                    for col in response["ResultSet"]["Rows"][0]["Data"]
                ]
                results.append(columns)  # Add the header row
                results.extend(
                    [
                        [
                            cell.get("VarCharValue", None) for cell in row["Data"]
                        ]  # Use get to avoid KeyError
                        for row in response["ResultSet"]["Rows"][1:]
                    ]
                )

                next_token = response.get("NextToken")
                if not next_token:
                    break

        return results

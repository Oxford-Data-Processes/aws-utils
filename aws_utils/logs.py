import boto3
import json
from datetime import datetime
import pytz
from typing import List, Dict, Any


class LogsHandler:
    def log_action(
        self, bucket_name: str, project_name: str, action: str, user: str
    ) -> None:
        """
        Logs an action performed by a user to an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket where logs will be stored.
            project_name (str): The name of the project associated with the log entry.
            action (str): The action being logged.
            user (str): The user who performed the action.
        """
        s3_client = boto3.client("s3")

        timestamp = datetime.now(pytz.timezone("Europe/London")).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        log_entry = {"timestamp": timestamp, "action": action, "user": user}

        log_file_name = f"logs/{project_name}/{timestamp}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=log_file_name,
            Body=json.dumps([log_entry]) + "\n",
            ContentType="application/json",
        )

    def get_logs(self, bucket_name: str, project_name: str) -> List[Dict[str, Any]]:
        """
        Retrieves logs from an S3 bucket for a specified project.

        Args:
            bucket_name (str): The name of the S3 bucket from which to retrieve logs.
            project_name (str): The name of the project whose logs are to be retrieved.

        Returns:
            List[Dict[str, Any]]: A list of log entries for the specified project.
        """
        s3_client = boto3.client("s3")
        log_prefix = f"logs/{project_name}/"

        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=log_prefix)
        logs = response.get("Contents", [])

        if not logs:
            return []

        log_data: List[Dict[str, Any]] = []

        for log in logs:
            log_key = log["Key"]
            log_object = s3_client.get_object(Bucket=bucket_name, Key=log_key)
            log_content = json.loads(log_object["Body"].read().decode("utf-8"))[0]

            log_data.append(log_content)

        return log_data

from datetime import datetime
import pytz
from typing import List, Dict, Any
from aws_utils.s3 import S3Handler


class LogsHandler:
    def log_action(
        self, bucket_name: str, project_name: str, action: str, user: str
    ) -> None:
        """
        Logs an action performed by a user to an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket where the log entry will be stored.
            project_name (str): The name of the project associated with the log entry.
            action (str): A description of the action being logged.
            user (str): The identifier of the user who performed the action.
        """
        timestamp = datetime.now(pytz.timezone("Europe/London")).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
        log_entry = {"timestamp": timestamp, "action": action, "user": user}

        log_file_name = f"logs/{project_name}/{timestamp}.json"

        s3_handler = S3Handler()
        s3_handler.upload_json_to_s3(
            bucket_name=bucket_name,
            json_key=log_file_name,
            json_data=log_entry,
        )

    def get_logs(self, bucket_name: str, project_name: str) -> List[Dict[str, Any]]:
        """
        Retrieves logs from an S3 bucket for a specified project.

        Args:
            bucket_name (str): The name of the S3 bucket from which to retrieve log entries.
            project_name (str): The name of the project whose logs are to be retrieved.

        Returns:
            List[Dict[str, Any]]: A list of log entries for the specified project,
            where each entry is represented as a dictionary.
        """
        s3_handler = S3Handler()
        log_prefix = f"logs/{project_name}/"
        logs = s3_handler.list_objects(bucket_name=bucket_name, prefix=log_prefix)

        if not logs:
            return []

        log_data: List[Dict[str, Any]] = []

        for log in logs:
            log_key = log["Key"]
            log_object = s3_handler.load_json_from_s3(
                bucket_name=bucket_name, json_key=log_key
            )
            log_data.append(log_object[0])

        return log_data

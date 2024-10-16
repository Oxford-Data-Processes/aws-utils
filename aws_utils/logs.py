import boto3
import json
from datetime import datetime
import pytz


class LogsHandler:
    def log_action(self, bucket_name, project_name, action, user):
        s3_client = boto3.client("s3")

        timestamp = datetime.now(pytz.timezone("Europe/London")).isoformat()
        log_entry = {"timestamp": timestamp, "action": action, "user": user}

        log_file_name = f"logs/{project_name}/{datetime.now(pytz.timezone('Europe/London')).strftime('%Y-%m-%dT%H:%M:%S')}.json"

        s3_client.put_object(
            Bucket=bucket_name,
            Key=log_file_name,
            Body=json.dumps([log_entry]) + "\n",
            ContentType="application/json",
        )

    def get_logs(self, bucket_name, project_name):
        s3_client = boto3.client("s3")
        log_prefix = f"logs/{project_name}/"

        # List objects in the specified S3 bucket with the log prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=log_prefix)
        logs = response.get("Contents", [])

        if not logs:
            return []

        log_data = []

        for log in logs:
            log_key = log["Key"]
            # Fetch and display log content
            log_object = s3_client.get_object(Bucket=bucket_name, Key=log_key)
            log_content = json.loads(log_object["Body"].read().decode("utf-8"))[0]

            log_data.append(log_content)

        return log_data

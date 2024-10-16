import boto3
import json
from datetime import datetime
import pytz


class LogsHandler:
    def log_action(self, bucket_name, action, user):
        s3_client = boto3.client("s3")

        timestamp = datetime.now().isoformat()
        log_entry = {"timestamp": timestamp, "action": action, "user": user}

        london_tz = pytz.timezone("Europe/London")
        log_file_name = (
            f"logs/{datetime.now(london_tz).strftime('%Y-%m-%dT%H:%M:%S')}.json"
        )

        s3_client.put_object(
            Bucket=bucket_name,
            Key=log_file_name,
            Body=json.dumps([log_entry]) + "\n",
            ContentType="application/json",
        )

import os
import boto3
import re
from typing import List, Dict, Optional


class SQSHandler:
    def __init__(self) -> None:
        """
        Initializes the SQSHandler by creating an SQS client using AWS credentials
        from environment variables.
        """
        self.sqs_client = boto3.client(
            "sqs",
            region_name=os.environ["AWS_REGION"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            aws_session_token=os.environ["AWS_SESSION_TOKEN"],
        )

    @staticmethod
    def extract_datetime_from_sns_message(message: str) -> Optional[str]:
        """
        Extracts a datetime string from the given SNS message.

        Args:
            message (str): The SNS message from which to extract the datetime.

        Returns:
            Optional[str]: The extracted datetime string if found, otherwise None.
        """
        match = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", message)
        return match.group(0) if match else None

    def delete_all_sqs_messages(self, queue_url: str) -> None:
        """
        Deletes all messages from the specified SQS queue.

        Args:
            queue_url (str): The URL of the SQS queue from which to delete messages.
        """
        all_messages = self.get_all_sqs_messages(queue_url)
        for message in all_messages:
            self.sqs_client.delete_message(
                QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
            )

    def get_all_sqs_messages(self, queue_url: str) -> List[Dict[str, Optional[str]]]:
        """
        Retrieves all messages from the specified SQS queue.

        Args:
            queue_url (str): The URL of the SQS queue from which to retrieve messages.

        Returns:
            List[Dict[str, Optional[str]]]: A list of dictionaries containing the
            timestamp and message body for each message.
        """
        all_messages = []
        try:
            while True:
                response = self.sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=5,
                    MessageAttributeNames=["All"],
                )
                messages = response.get("Messages", [])

                if not messages:
                    break

                for message in messages:
                    timestamp = self.extract_datetime_from_sns_message(message["Body"])
                    message_body = message["Body"]
                    all_messages.append(
                        {"timestamp": timestamp, "message": message_body}
                    )

            if not all_messages:
                return []

            all_messages.sort(key=lambda x: x["timestamp"])
            return all_messages
        except Exception as e:
            raise Exception(e)

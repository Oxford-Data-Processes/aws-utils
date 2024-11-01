import boto3
import os
from typing import Any


class SNSHandler:
    def __init__(self, topic_name: str) -> None:
        """
        Initializes the SNSHandler with the specified SNS topic name.

        Args:
            topic_name (str): The name of the SNS topic to which notifications will be sent.
        """
        self.sns_client = boto3.client("sns", region_name=os.environ["AWS_REGION"])
        self.topic_arn = (
            f"arn:aws:sns:eu-west-2:{os.environ['AWS_ACCOUNT_ID']}:{topic_name}"
        )

    def send_notification(self, message: str, subject: str) -> None:
        """
        Sends a notification to the specified SNS topic.

        Args:
            message (str): The message to be sent in the notification.
            subject (str): The subject of the notification.
        """
        self.sns_client.publish(
            TopicArn=self.topic_arn,
            Message=message,
            Subject=subject,
        )

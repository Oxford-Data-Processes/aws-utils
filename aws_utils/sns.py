import boto3
import os


class SNSHandler:
    def __init__(self):
        self.sns_client = boto3.client("sns", region_name=os.environ["AWS_REGION"])
        self.topic_arn = f"arn:aws:sns:eu-west-2:{os.environ["AWS_ACCOUNT_ID"]}:rtg-automotive-stock-notifications"

    def send_notification(self, message):
        self.sns_client.publish(
            TopicArn=self.topic_arn,
            Message=message,
            Subject="Stock Feed Processed",
        )

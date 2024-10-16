import os
import boto3
import re


class SQSHandler:
    def __init__(self):
        self.sqs_client = boto3.client(
            "sqs",
            region_name=os.environ["AWS_REGION"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            aws_session_token=os.environ["AWS_SESSION_TOKEN"],
        )

    @staticmethod
    def extract_datetime_from_sns_message(message):
        # Regular expression to find the datetime in the message
        match = re.search(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", message)
        return match.group(0) if match else None

    def get_all_sqs_messages(self, queue_url):
        all_messages = []
        try:
            while True:
                # Receive messages from the SQS queue
                response = self.sqs_client.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=5,
                    MessageAttributeNames=["All"],
                )

                # Check if there are any messages
                messages = response.get("Messages", [])
                if not messages:
                    print("No more messages in the queue.")
                    break  # Exit the loop if no more messages

                for message in messages:
                    timestamp = self.extract_datetime_from_sns_message(message["Body"])
                    message_body = message["Body"]
                    all_messages.append(
                        {"timestamp": timestamp, "message": message_body}
                    )

            if not all_messages:
                raise Exception("No messages were retrieved from the queue.")

            all_messages.sort(key=lambda x: x["timestamp"])
            return all_messages
        except Exception as e:
            raise Exception(f"An error occurred while retrieving messages: {e}")

from aws_utils import sqs, iam
import streamlit as st
import os


aws_access_key_id = st.secrets["aws_credentials"]["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = st.secrets["aws_credentials"]["AWS_SECRET_ACCESS_KEY"]
aws_account_id = st.secrets["aws_credentials"]["AWS_ACCOUNT_ID"]

aws_credentials = iam.AWSCredentials(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    stage="dev",
)

aws_credentials.get_aws_credentials()

sqs_handler = sqs.SQSHandler()


sqs_queue_url = (
    f"https://sqs.eu-west-2.amazonaws.com/{aws_account_id}/rtg-automotive-sqs-queue"
)


# def delete_all_sqs_messages(queue_url):
#     try:
#         while True:
#             # Receive messages from the SQS queue
#             response = sqs_handler.sqs_client.receive_message(
#                 QueueUrl=queue_url,
#                 MaxNumberOfMessages=10,
#                 WaitTimeSeconds=5,
#             )
#             messages = response.get("Messages", [])

#             if not messages:
#                 break

#             for message in messages:
#                 # Delete each message from the queue
#                 sqs_handler.sqs_client.delete_message(
#                     QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
#                 )
#                 print(f"Deleted message: {message['MessageId']}")

#         print("All messages deleted from the queue.")
#     except Exception as e:
#         print(f"An error occurred while deleting messages: {e}")


# # Call the function to delete all messages
# delete_all_sqs_messages(sqs_queue_url)

messages = sqs_handler.get_all_sqs_messages(sqs_queue_url)

print(messages)

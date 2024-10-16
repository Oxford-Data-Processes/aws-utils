import boto3
import time
import os


def get_aws_credentials(aws_account_id, role):
    role_arn = f"arn:aws:iam::{aws_account_id}:role/{role}"
    session_name = f"MySession-{int(time.time())}"

    sts_client = boto3.client(
        "sts",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    # Assume the role
    response = sts_client.assume_role(RoleArn=role_arn, RoleSessionName=session_name)
    # Extract the credentials
    credentials = response["Credentials"]
    access_key_id = credentials["AccessKeyId"]
    secret_access_key = credentials["SecretAccessKey"]
    session_token = credentials["SessionToken"]

    os.environ["AWS_ACCESS_KEY_ID"] = access_key_id
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_access_key
    os.environ["AWS_SESSION_TOKEN"] = session_token

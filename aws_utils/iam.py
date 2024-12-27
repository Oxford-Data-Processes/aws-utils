import boto3
import time
import os
from typing import Literal, Dict


def get_aws_credentials(variables: Dict[str, str]) -> None:
    stage = variables["STAGE"]
    aws_access_key_id = variables["AWS_ACCESS_KEY_ID_ADMIN"]
    aws_secret_access_key = variables["AWS_SECRET_ACCESS_KEY_ADMIN"]

    aws_account_id = get_aws_account_id(stage)
    role = get_iam_role(stage)
    aws_region = "eu-west-2"
    role_arn = f"arn:aws:iam::{aws_account_id}:role/{role}"

    session_name = f"MySession-{int(time.time())}"

    sts_client = boto3.client(
        "sts",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    response = sts_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=session_name,
    )

    credentials = response["Credentials"]
    os.environ["AWS_ACCESS_KEY_ID"] = credentials["AccessKeyId"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials["SecretAccessKey"]
    os.environ["AWS_SESSION_TOKEN"] = credentials["SessionToken"]
    os.environ["AWS_REGION"] = aws_region
    os.environ["AWS_ACCOUNT_ID"] = aws_account_id


def get_iam_role(stage: Literal["dev", "prod"]) -> str:
    if stage == "dev":
        return "DevAdminRole"
    elif stage == "prod":
        return "ProdAdminRole"


def get_aws_account_id(stage: Literal["dev", "prod"]) -> str:
    if stage == "dev":
        return "654654324108"
    elif stage == "prod":
        return "905418370160"

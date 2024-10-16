import boto3
import time
import os


class IAM:
    def __init__(self, stage="dev"):
        self.stage = stage

    def get_iam_role(self):
        if self.stage == "dev":
            return "DevAdminRole"
        elif self.stage == "prod":
            return "ProdAdminRole"
        else:
            raise ValueError(f"Invalid stage: {self.stage}")

    def get_aws_account_id(self):
        if self.stage == "dev":
            return "654654324108"
        elif self.stage == "prod":
            return "905418370160"
        else:
            raise ValueError(f"Invalid stage: {self.stage}")


class AWSCredentials(IAM):
    def __init__(self, aws_access_key_id, aws_secret_access_key, stage="dev"):
        super().__init__(stage)
        self.aws_account_id = self.get_aws_account_id()
        self.aws_region = "eu-west-2"
        self.role = self.get_iam_role()
        self.role_arn = f"arn:aws:iam::{self.aws_account_id}:role/{self.role}"
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def get_aws_credentials(self):
        session_name = f"MySession-{int(time.time())}"

        sts_client = boto3.client(
            "sts",
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

        response = sts_client.assume_role(
            RoleArn=self.role_arn, RoleSessionName=session_name
        )

        credentials = response["Credentials"]
        aws_access_key_id = credentials["AccessKeyId"]
        aws_secret_access_key = credentials["SecretAccessKey"]
        aws_session_token = credentials["SessionToken"]

        os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
        os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
        os.environ["AWS_SESSION_TOKEN"] = aws_session_token
        os.environ["AWS_REGION"] = self.aws_region
        os.environ["AWS_ACCOUNT_ID"] = self.aws_account_id

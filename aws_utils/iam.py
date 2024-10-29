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
    @staticmethod
    def get_aws_credentials(aws_access_key_id, aws_secret_access_key, iam_instance):

        aws_account_id = iam_instance.get_aws_account_id()
        role = iam_instance.get_iam_role()
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
        os.environ["AWS_ACCESS_KEY_ID_ADMIN"] = ""
        os.environ["AWS_SECRET_ACCESS_KEY_ADMIN"] = ""

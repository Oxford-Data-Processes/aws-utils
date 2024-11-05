import boto3
import time
import os
from typing import Literal, Dict


def get_aws_credentials(variables: Dict[str, str]) -> None:
    iam_instance = IAM(stage=variables["STAGE"])
    AWSCredentials.get_aws_credentials(
        aws_access_key_id=variables["AWS_ACCESS_KEY_ID_ADMIN"],
        aws_secret_access_key=variables["AWS_SECRET_ACCESS_KEY_ADMIN"],
        iam_instance=iam_instance,
    )


class IAM:
    def __init__(self, stage: Literal["dev", "prod"] = "dev") -> None:
        """
        Initializes the IAM class with the specified stage.

        Args:
            stage (Literal["dev", "prod"]): The stage of the environment, either 'dev' or 'prod'.
        """
        self.stage = stage

    def get_iam_role(self) -> str:
        """
        Retrieves the IAM role based on the current stage.

        Returns:
            str: The IAM role name corresponding to the stage.

        Raises:
            ValueError: If the stage is not 'dev' or 'prod'.
        """
        if self.stage == "dev":
            return "DevAdminRole"
        elif self.stage == "prod":
            return "ProdAdminRole"
        else:
            raise ValueError(f"Invalid stage: {self.stage}")

    def get_aws_account_id(self) -> str:
        """
        Retrieves the AWS account ID based on the current stage.

        Returns:
            str: The AWS account ID corresponding to the stage.

        Raises:
            ValueError: If the stage is not 'dev' or 'prod'.
        """
        if self.stage == "dev":
            return "654654324108"
        elif self.stage == "prod":
            return "905418370160"
        else:
            raise ValueError(f"Invalid stage: {self.stage}")


class AWSCredentials(IAM):
    @staticmethod
    def get_aws_credentials(
        aws_access_key_id: str, aws_secret_access_key: str, iam_instance: IAM
    ) -> None:
        """
        Assumes an IAM role and sets the AWS credentials in the environment variables.

        Args:
            aws_access_key_id (str): The AWS access key ID.
            aws_secret_access_key (str): The AWS secret access key.
            iam_instance (IAM): An instance of the IAM class to retrieve role and account ID.

        Raises:
            Exception: If the role assumption fails.
        """
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

import boto3
import os
import time


class LambdaHandler:
    def trigger_lambda_function(self, function_name: str) -> bool:
        lambda_client = boto3.client(
            "lambda",
            region_name="eu-west-2",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            aws_session_token=os.environ["AWS_SESSION_TOKEN"],
        )
        try:
            response = lambda_client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
            )
            time.sleep(2)
            return True
        except Exception as e:
            st.error(f"Error: {str(e)}")
            return False

from aws_utils import iam, logs
import streamlit as st

# Test parameters
bucket_name = "rtg-automotive-bucket-654654324108"
project_name = "frontend"

# Initialize AWS credentials
aws_access_key_id = st.secrets["aws_credentials"]["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = st.secrets["aws_credentials"]["AWS_SECRET_ACCESS_KEY"]
aws_account_id = st.secrets["aws_credentials"]["AWS_ACCOUNT_ID"]

aws_credentials = iam.AWSCredentials(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    stage="dev",
)

aws_credentials.get_aws_credentials()

# Test for get_logs function
logs_handler = logs.LogsHandler()
logs = logs_handler.get_logs(bucket_name, project_name)

# Print the logs to verify the output
print(logs)  # This will display the logs fetched from S3

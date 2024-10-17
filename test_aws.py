from aws_utils import iam, logs
import streamlit as st
import os

# Test parameters
bucket_name = "rtg-automotive-bucket-654654324108"
project_name = "frontend"

# Initialize AWS credentials
os.environ["AWS_ACCESS_KEY_ID_ADMIN"] = st.secrets["aws_credentials"][
    "AWS_ACCESS_KEY_ID"
]
os.environ["AWS_SECRET_ACCESS_KEY_ADMIN"] = st.secrets["aws_credentials"][
    "AWS_SECRET_ACCESS_KEY"
]

iam_instance = iam.IAM(stage=os.environ["STAGE"])
iam.AWSCredentials.get_aws_credentials(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID_ADMIN"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY_ADMIN"],
    iam_instance=iam_instance,
)
# Test for get_logs function
logs_handler = logs.LogsHandler()
logs = logs_handler.get_logs(bucket_name, project_name)

# Print the logs to verify the output
print(logs)  # This will display the logs fetched from S3

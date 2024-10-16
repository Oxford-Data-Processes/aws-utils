from aws_utils import s3, iam, athena
import streamlit as st
import os
import pandas as pd

aws_access_key_id = st.secrets["aws_credentials"]["AWS_ACCESS_KEY_ID"]
aws_secret_access_key = st.secrets["aws_credentials"]["AWS_SECRET_ACCESS_KEY"]
aws_account_id = st.secrets["aws_credentials"]["AWS_ACCOUNT_ID"]

aws_credentials = iam.AWSCredentials(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    stage="dev",
)

aws_credentials.get_aws_credentials()


athena_query_executor = athena.AthenaQueryExecutor(
    database="rtg_automotive",
    workgroup="rtg-automotive-workgroup",
    output_bucket=f"rtg-automotive-bucket-{aws_account_id}",
)

query = """SELECT * FROM "rtg_automotive"."store" WHERE ebay_store = 'RTG' AND supplier = 'RTG' LIMIT 10;"""
response_csv = athena_query_executor.run_query(query)

df = pd.DataFrame(
    response_csv[1:], columns=response_csv[0]
)  # Create DataFrame from response
df.to_csv("response_data.csv", index=False)  # Write DataFrame to CSV

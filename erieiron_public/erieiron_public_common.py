import os

import boto3
import botocore.session


def get_secret_arn(secret_id):
    aws_credentials = botocore.session.Session(
        profile=os.environ.get("AWS_PROFILE")
    ).get_credentials().get_frozen_credentials()
    
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    aws_secrets_client = boto3.client(
        "secretsmanager",
        region_name=region
    )
    
    response = aws_secrets_client.get_secret_value(SecretId=secret_id)
    return response.get("ARN")

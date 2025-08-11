import json
import os
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError


@lru_cache(maxsize=1)
def get_secret_json(secret_arn: str) -> dict:
    """Fetch and parse a JSON secret from AWS Secrets Manager. Fails fast on errors."""
    client = boto3.client("secretsmanager")  # Region from env/IMDS unless overridden elsewhere
    try:
        resp = client.get_secret_value(SecretId=secret_arn)
    except ClientError as e:
        raise RuntimeError(f"Failed to fetch RDS secret {secret_arn}: {e}") from e
    
    secret_str = resp.get("SecretString")
    if not secret_str:
        raise RuntimeError(f"Secret {secret_arn} returned empty SecretString")
    
    try:
        data = json.loads(secret_str)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Secret {secret_arn} is not valid JSON: {e}") from e
    
    required_keys = ["username", "password", "host", "port", "database"]
    missing = [k for k in required_keys if k not in data]
    if missing:
        raise RuntimeError(f"Secret {secret_arn} missing keys: {missing}")
    
    return data


def get_secret_from_env_arn(env_var_name):
    secret_arn = os.getenv(env_var_name)
    if not secret_arn:
        raise ValueError(f"no env var found for {env_var_name}")
    
    secret_json = get_secret_json(secret_arn)
    if not secret_json:
        raise ValueError(f"no secret data found for {secret_arn}")
    
    return secret_json

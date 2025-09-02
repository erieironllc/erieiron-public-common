import json
import os
from functools import lru_cache

import boto3


def get_django_settings_databases_conf(region_name: str = None) -> dict:
    """
    Build Django DATABASES configuration from an RDS secret stored in AWS Secrets Manager.

    Parameters:
    - region_name: str | None
      AWS region for secret lookup. Defaults to AWS_DEFAULT_REGION.

    Returns:
    - dict
      A dict suitable for Django's DATABASES setting with a 'default' connection.
    """
    rds_secret = get_secret_from_env_arn("RDS_SECRET_ARN")
    
    return {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": rds_secret["dbname"],
            "USER": rds_secret["username"],
            "PASSWORD": rds_secret["password"],
            "HOST": rds_secret["host"],
            "PORT": int(rds_secret["port"]),
            "CONN_MAX_AGE": int(os.getenv("DJANGO_DB_CONN_MAX_AGE", "60")),
            "TEST": {
                "NAME": rds_secret["dbname"]
            },
        }
    }


def get_secret_from_env_arn(env_var_name: str, region_name: str = None) -> dict:
    """
    Load a secret from AWS Secrets Manager by looking up its ARN from an environment variable.

    Parameters:
    - env_var_name: str
      Name of the environment variable that contains the secret ARN.
    - region_name: str | None
      AWS region to use. If not provided, defaults to AWS_DEFAULT_REGION.

    Returns:
    - dict
      Parsed JSON contents of the secret.

    Raises:
    - ValueError: If the environment variable is not set or the secret has no data.
    """
    secret_arn = os.getenv(env_var_name)
    if not secret_arn:
        raise ValueError(f"no env var found for {env_var_name}")
    
    secret_json = get_secret_json(secret_arn, region_name)
    if not secret_json:
        raise ValueError(f"no secret data found for {secret_arn}")
    
    return secret_json


@lru_cache(maxsize=1)
def get_secret_json(secret_arn: str, region_name: str = None) -> dict:
    """
    Retrieve a secret from AWS Secrets Manager and return it as a JSON dict.

    Parameters:
    - secret_arn: str
      The full ARN of the secret in AWS Secrets Manager.
    - region_name: str | None
      AWS region to use. If not provided, the environment variable AWS_DEFAULT_REGION is used.

    Returns:
    - dict
      Parsed JSON contents of the secret.

    Raises:
    - ValueError: If region_name is not provided and AWS_DEFAULT_REGION is not set.
    - json.JSONDecodeError: If the secret string is not valid JSON.
    """
    region_name = region_name or os.getenv("AWS_DEFAULT_REGION")
    if not region_name:
        raise ValueError(f"unable to identify aws region.  not found in param or in env AWS_DEFAULT_REGION")
    
    secret_string = boto3.client(
        "secretsmanager",
        region_name=region_name
    ).get_secret_value(
        SecretId=secret_arn
    ).get("SecretString")
    
    return json.loads(secret_string)

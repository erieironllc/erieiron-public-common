import json
import os
from functools import lru_cache

import boto3


@lru_cache(maxsize=1)
def get_secret_json(secret_arn: str, region_name: str) -> dict:
    client = boto3.client("secretsmanager", region_name=region_name)
    resp = client.get_secret_value(SecretId=secret_arn)
    secret_str = resp.get("SecretString")
    return json.loads(secret_str)


def get_django_settings_databases_conf(region_name: str) -> dict:
    rds_secret = get_secret_from_env_arn("RDS_SECRET_ARN", region_name)
    return {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": rds_secret["dbname"],
            "USER": rds_secret["username"],
            "PASSWORD": rds_secret["password"],
            "HOST": rds_secret["host"],
            "PORT": int(rds_secret["port"]),
            "CONN_MAX_AGE": int(os.getenv("DJANGO_DB_CONN_MAX_AGE", "60")),
        }
    }


def get_secret_from_env_arn(env_var_name: str, region_name: str) -> dict:
    secret_arn = os.getenv(env_var_name)
    if not secret_arn:
        raise ValueError(f"no env var found for {env_var_name}")
    
    secret_json = get_secret_json(secret_arn, region_name)
    if not secret_json:
        raise ValueError(f"no secret data found for {secret_arn}")
    
    return secret_json

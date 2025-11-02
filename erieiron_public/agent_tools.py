import json
import os
from functools import lru_cache
from pathlib import Path

import boto3
import pg8000
import yaml


def parse_cloudformation_yaml(cloudformation_yaml) -> dict:
    class CloudFormationLoader(yaml.SafeLoader):
        pass
    
    for tag in ["!Select", "!Ref", "!Sub", "!GetAtt", "!Join", "!If", "!Equals", "!And", "!Or", "!Not", "!FindInMap", "!ImportValue"]:
        CloudFormationLoader.add_constructor(tag, lambda loader, node: node.value)
    
    if isinstance(cloudformation_yaml, Path):
        cloudformation_yaml = cloudformation_yaml.read_text()
    
    return yaml.load(
        cloudformation_yaml,
        Loader=CloudFormationLoader
    )


def get_pg8000_connection(region_name: str = None):
    """Return a pg8000 connection that matches get_database_conf().

    The returned connection can be used as a context manager to ensure the
    handle is closed, e.g.:

    ```python
    with get_pg8000_connection(region) as conn:
        conn.cursor().execute("SELECT 1")
    ```
    """
    region_name = region_name or os.getenv("AWS_DEFAULT_REGION")
    db_conf = get_database_conf(region_name)
    
    connection_kwargs = {
        "user": db_conf.get("USER"),
        "password": db_conf.get("PASSWORD"),
        "host": db_conf.get("HOST"),
        "port": db_conf.get("PORT"),
        "database": db_conf.get("NAME"),
    }
    
    missing = [key for key, value in connection_kwargs.items() if value in (None, "")]
    if missing:
        missing_display = ", ".join(missing)
        raise ValueError(f"missing database configuration values: {missing_display}")
    
    connection = pg8000.connect(**connection_kwargs)
    return _ensure_pg8000_connection_context_manager(connection)


def _ensure_pg8000_connection_context_manager(connection):
    """Attach __enter__/__exit__ to pg8000 connection instances when missing."""
    connection_cls = connection.__class__
    has_enter = getattr(connection_cls, "__enter__", None)
    has_exit = getattr(connection_cls, "__exit__", None)
    
    if has_enter and has_exit:
        return connection
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
    
    setattr(connection_cls, "__enter__", __enter__)
    setattr(connection_cls, "__exit__", __exit__)
    return connection


def get_database_conf(region_name: str = None) -> dict:
    """
    Build Django DATABASES configuration from an RDS secret stored in AWS Secrets Manager.

    Parameters:
    - region_name: str | None
      AWS region for secret lookup. Defaults to AWS_DEFAULT_REGION.

    Returns:
    - dict
      A dict suitable for Django's DATABASES setting with a 'default' connection.
    """
    
    if os.environ.get("LOCAL_DB_NAME"):
        return {
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": os.environ.get("LOCAL_DB_NAME"),
                "HOST": "localhost",
                "PORT": "5432",
            }
        }
    else:
        rds_secret = get_secret_from_env_arn("RDS_SECRET_ARN", region_name)
        
        return {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.getenv("ERIEIRON_DB_NAME"),
            "HOST": os.getenv("ERIEIRON_DB_HOST"),
            "PORT": int(os.getenv("ERIEIRON_DB_PORT", "5432")),
            "USER": rds_secret.get("username"),
            "PASSWORD": rds_secret.get("password"),
            "CONN_MAX_AGE": int(os.getenv("DJANGO_DB_CONN_MAX_AGE", "60")),
            "TEST": {
                "NAME": rds_secret.get("dbname")
            }
        }


def get_django_settings_databases_conf(region_name: str = None) -> dict:
    return {
        "default": get_database_conf(region_name)
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

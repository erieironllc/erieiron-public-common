import json
import logging
import os
import threading
import time
from functools import lru_cache
from pathlib import Path
from typing import Dict, Tuple

import boto3
import pg8000
import yaml

logger = logging.getLogger(__name__)


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


def get_pg8000_connection(region_name: str = None, force_secret_refresh: bool = False):
    """Return a pg8000 connection that matches get_database_conf().

    Parameters
    ----------
    region_name:
        Explicit AWS region for the secret lookup. Falls back to AWS_DEFAULT_REGION.
    force_secret_refresh:
        When True, bypasses the cache and forces a fresh Secrets Manager read.

    The returned connection can be used as a context manager to ensure the
    handle is closed, e.g.:

    ```python
    with get_pg8000_connection(region) as conn:
        conn.cursor().execute("SELECT 1")
    ```
    """
    region_name = region_name or os.getenv("AWS_DEFAULT_REGION")
    db_conf = get_database_conf(
        region_name=region_name,
        force_secret_refresh=force_secret_refresh,
        include_credentials=True
    )
    
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


def get_database_conf(
        region_name: str = None,
        *,
        force_secret_refresh: bool = False,
        include_credentials: bool = False
) -> dict:
    """
    Build Django DATABASES configuration from an RDS secret stored in AWS Secrets Manager.

    Parameters:
    - region_name: str | None
      AWS region for secret lookup. Defaults to AWS_DEFAULT_REGION.
    - force_secret_refresh: bool
      If True, bypasses the cache and pulls a fresh copy of the secret.
    - include_credentials: bool
      When True, embeds "USER"/"PASSWORD" in the returned dict for helpers
      like `get_pg8000_connection` that connect outside of Django.

    Returns:
    - dict
      A dict suitable for Django's DATABASES setting with a 'default' connection.
      When using the remote RDS secret, the ENGINE points at
      `erieiron_public.db.backends.dynamic_postgresql`, which refreshes the
      password on every new connection.
    """
    
    if os.environ.get("LOCAL_DB_NAME"):
        return {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": os.environ.get("LOCAL_DB_NAME"),
            "HOST": "localhost",
            "PORT": "5432",
        }
    
    rds_secret = get_secret_from_env_arn(
        "RDS_SECRET_ARN",
        region_name,
        force_refresh=force_secret_refresh
    )
    conn_max_age = int(os.getenv("DJANGO_DB_CONN_MAX_AGE", "0"))
    database_name = os.getenv("ERIEIRON_DB_NAME") or rds_secret.get("dbname")
    resolved_region = _resolve_region(region_name)
    
    db_conf = {
        "ENGINE": "erieiron_public.db.backends.dynamic_postgresql",
        "NAME": database_name,
        "HOST": os.getenv("ERIEIRON_DB_HOST"),
        "PORT": int(os.getenv("ERIEIRON_DB_PORT", "5432")),
        "CONN_MAX_AGE": conn_max_age,
        "RDS_SECRET_REGION_NAME": resolved_region,
        "TEST": {
            "NAME": database_name
        }
    }
    
    if include_credentials:
        db_conf.update(
            {
                "USER": rds_secret.get("username"),
                "PASSWORD": rds_secret.get("password"),
            }
        )
    
    return db_conf


def get_django_settings_databases_conf(region_name: str = None) -> dict:
    return {
        "default": get_database_conf(region_name=region_name)
    }


def get_secret_from_env_arn(
        env_var_name: str,
        region_name: str = None,
        *,
        force_refresh: bool = False
) -> dict:
    """
    Load a secret from AWS Secrets Manager by looking up its ARN from an environment variable.

    Parameters:
    - env_var_name: str
      Name of the environment variable that contains the secret ARN.
    - region_name: str | None
      AWS region to use. If not provided, defaults to AWS_DEFAULT_REGION.

    Returns:
    - dict
      Parsed JSON contents of the secret. Results are cached according to
      AWS_SECRET_CACHE_TTL_SECONDS unless force_refresh is True.

    Raises:
    - ValueError: If the environment variable is not set or the secret has no data.
    """
    secret_arn = os.getenv(env_var_name)
    if not secret_arn:
        raise ValueError(f"no env var found for {env_var_name}")
    
    secret_json = get_secret_json(secret_arn, region_name, force_refresh=force_refresh)
    if not secret_json:
        raise ValueError(f"no secret data found for {secret_arn}")
    
    return secret_json


def get_secret_json(
        secret_arn: str,
        region_name: str = None,
        *,
        force_refresh: bool = False
) -> dict:
    """
    Retrieve a secret from AWS Secrets Manager and return it as a JSON dict.

    Parameters:
    - secret_arn: str
      The full ARN of the secret in AWS Secrets Manager.
    - region_name: str | None
      AWS region to use. If not provided, the environment variable AWS_DEFAULT_REGION is used.

    Returns:
    - dict
      Parsed JSON contents of the secret. Cached for AWS_SECRET_CACHE_TTL_SECONDS
      unless force_refresh is specified.

    Raises:
    - ValueError: If region_name is not provided and AWS_DEFAULT_REGION is not set.
    - json.JSONDecodeError: If the secret string is not valid JSON.
    """
    cache = _get_secrets_manager_cache()
    return cache.get_secret(
        secret_arn=secret_arn,
        region_name=region_name,
        force_refresh=force_refresh
    )


def _resolve_region(region_name: str = None) -> str:
    """Return the AWS region, falling back to AWS_DEFAULT_REGION when needed."""
    resolved = region_name or os.getenv("AWS_DEFAULT_REGION")
    if not resolved:
        raise ValueError("unable to identify aws region. not found in param or in env AWS_DEFAULT_REGION")
    return resolved


@lru_cache(maxsize=1)
def _get_secrets_manager_cache() -> "SecretsManagerCache":
    ttl_value = os.getenv("AWS_SECRET_CACHE_TTL_SECONDS", "300")
    try:
        ttl_seconds = int(ttl_value)
    except ValueError as exc:
        raise ValueError("AWS_SECRET_CACHE_TTL_SECONDS must be an integer") from exc
    if ttl_seconds < 0:
        raise ValueError("AWS_SECRET_CACHE_TTL_SECONDS must be non-negative")
    return SecretsManagerCache(ttl_seconds=ttl_seconds)


class SecretsManagerCache:
    """Thread-safe TTL cache for AWS Secrets Manager payloads."""
    
    def __init__(self, ttl_seconds: int):
        self.ttl_seconds = ttl_seconds
        self._lock = threading.Lock()
        self._cache: Dict[Tuple[str, str], Tuple[float, dict]] = {}
    
    def get_secret(
            self,
            secret_arn: str,
            region_name: str = None,
            *,
            force_refresh: bool = False
    ) -> dict:
        region = _resolve_region(region_name)
        key = (secret_arn, region)
        now = time.monotonic()
        use_cache = self.ttl_seconds > 0 and not force_refresh
        
        if use_cache:
            with self._lock:
                cached = self._cache.get(key)
                if cached and cached[0] > now:
                    return cached[1].copy()
        
        secret_payload = self._fetch_secret(secret_arn, region)
        payload_copy = secret_payload.copy()
        
        if self.ttl_seconds > 0:
            expires_at = now + self.ttl_seconds
            with self._lock:
                self._cache[key] = (expires_at, secret_payload.copy())
        else:
            with self._lock:
                self._cache.pop(key, None)
        
        return payload_copy
    
    def _fetch_secret(self, secret_arn: str, region_name: str) -> dict:
        secret_string = boto3.client(
            "secretsmanager",
            region_name=region_name
        ).get_secret_value(
            SecretId=secret_arn
        ).get("SecretString")
        
        if not secret_string:
            raise ValueError(f"no secret data found for {secret_arn}")
        
        logger.info("Refreshed secret %s in region %s", secret_arn, region_name)
        return json.loads(secret_string)


@lru_cache(maxsize=1)
def get_cognito_config(force_refresh: bool = False) -> dict:
    secret_arn = os.environ.get("COGNITO_SECRET_ARN")
    if secret_arn:
        return _get_secrets_manager_cache().get_secret(
            secret_arn=secret_arn,
            force_refresh=force_refresh
        )
        
    # Fallback to individual env vars for backwards compatibility
    return {
        "userPoolId": os.environ.get("COGNITO_USER_POOL_ID"),
        "clientId": os.environ.get("COGNITO_CLIENT_ID"),
        "domain": os.environ.get("COGNITO_DOMAIN"),
    }

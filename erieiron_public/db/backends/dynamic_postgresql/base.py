"""Django PostgreSQL backend that refreshes credentials from Secrets Manager."""

from __future__ import annotations

import logging
from typing import Any, Dict

from django.db.backends.postgresql import base as postgres_base
from django.db.backends.postgresql.base import DatabaseWrapper as PostgresDatabaseWrapper
from django.db.utils import OperationalError

from erieiron_public.agent_tools import get_secret_from_env_arn


logger = logging.getLogger(__name__)

# Re-export the stock PostgreSQL helpers so Django can reuse all of its
# schema, feature, and client logic without duplication.
Database = postgres_base.Database
DatabaseClient = postgres_base.DatabaseClient
DatabaseCreation = postgres_base.DatabaseCreation
DatabaseFeatures = postgres_base.DatabaseFeatures
DatabaseIntrospection = postgres_base.DatabaseIntrospection
DatabaseOperations = postgres_base.DatabaseOperations
DatabaseSchemaEditor = postgres_base.DatabaseSchemaEditor

__all__ = [
    "Database",
    "DatabaseClient",
    "DatabaseCreation",
    "DatabaseFeatures",
    "DatabaseIntrospection",
    "DatabaseOperations",
    "DatabaseSchemaEditor",
    "DatabaseWrapper",
]


class DatabaseWrapper(PostgresDatabaseWrapper):
    """Override connection handling to pull fresh credentials each time."""

    def get_connection_params(self) -> Dict[str, Any]:
        params = super().get_connection_params()
        return self._inject_credentials(params, force_refresh=False)

    def get_new_connection(self, conn_params: Dict[str, Any]):
        try:
            return super().get_new_connection(conn_params)
        except OperationalError:
            logger.exception(
                "Database connection failed; refreshing Secrets Manager credentials and retrying"
            )
            refreshed_params = self._inject_credentials(
                conn_params.copy(),
                force_refresh=True,
            )
            return super().get_new_connection(refreshed_params)

    def _inject_credentials(
        self,
        conn_params: Dict[str, Any],
        *,
        force_refresh: bool,
    ) -> Dict[str, Any]:
        secret = get_secret_from_env_arn(
            "RDS_SECRET_ARN",
            region_name=self.settings_dict.get("RDS_SECRET_REGION_NAME"),
            force_refresh=force_refresh,
        )

        username = secret.get("username")
        password = secret.get("password")

        if username:
            conn_params["user"] = username
        if password:
            conn_params["password"] = password

        return conn_params

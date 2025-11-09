import importlib
from unittest import TestCase, skipUnless
from unittest.mock import patch

import django
from django.apps import apps
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.db.utils import OperationalError


if not settings.configured:
    settings.configure(
        USE_TZ=False,
        TIME_ZONE="UTC",
        INSTALLED_APPS=[],
        SECRET_KEY="test-key",
    )

if not apps.ready:
    django.setup()


try:
    from erieiron_public.db.backends.dynamic_postgresql import DatabaseWrapper as DynamicDatabaseWrapper
except ImproperlyConfigured:
    DynamicDatabaseWrapper = None
    POSTGRES_DRIVER_AVAILABLE = False
else:
    POSTGRES_DRIVER_AVAILABLE = True


class DynamicPostgreSQLBackendTests(TestCase):
    maxDiff = None

    def _settings(self):
        return {
            "ENGINE": "erieiron_public.db.backends.dynamic_postgresql",
            "NAME": "appdb",
            "HOST": "db.example.com",
            "PORT": "5432",
            "USER": "",
            "PASSWORD": "",
            "OPTIONS": {},
            "CONN_MAX_AGE": 0,
            "AUTOCOMMIT": True,
            "ATOMIC_REQUESTS": False,
            "RDS_SECRET_REGION_NAME": "us-west-2",
            "TIME_ZONE": "UTC",
        }

    @skipUnless(POSTGRES_DRIVER_AVAILABLE, "psycopg driver not installed")
    def test_backend_package_exposes_base_module(self):
        module = importlib.import_module(
            "erieiron_public.db.backends.dynamic_postgresql.base"
        )
        self.assertTrue(hasattr(module, "DatabaseWrapper"))

    @skipUnless(POSTGRES_DRIVER_AVAILABLE, "psycopg driver not installed")
    def test_get_connection_params_injects_secrets_manager_credentials(self):
        wrapper = DynamicDatabaseWrapper(self._settings(), alias="default")

        with patch(
            "erieiron_public.db.backends.dynamic_postgresql.base.PostgresDatabaseWrapper.get_connection_params"
        ) as mock_super_params, patch(
            "erieiron_public.db.backends.dynamic_postgresql.base.get_secret_from_env_arn"
        ) as mock_get_secret:
            mock_super_params.return_value = {
                "user": "fallback",
                "password": "fallback",
                "host": "db.example.com",
                "port": "5432",
                "database": "appdb",
            }
            mock_get_secret.return_value = {
                "username": "rotated-user",
                "password": "rotated-pass",
            }

            params = wrapper.get_connection_params()

        self.assertEqual(params["user"], "rotated-user")
        self.assertEqual(params["password"], "rotated-pass")
        mock_get_secret.assert_called_once_with(
            "RDS_SECRET_ARN",
            region_name="us-west-2",
            force_refresh=False,
        )

    @skipUnless(POSTGRES_DRIVER_AVAILABLE, "psycopg driver not installed")
    def test_get_new_connection_retries_with_fresh_secret(self):
        wrapper = DynamicDatabaseWrapper(self._settings(), alias="default")
        fresh_connection = object()

        with patch(
            "erieiron_public.db.backends.dynamic_postgresql.base.PostgresDatabaseWrapper.get_new_connection"
        ) as mock_super_conn, patch(
            "erieiron_public.db.backends.dynamic_postgresql.base.get_secret_from_env_arn"
        ) as mock_get_secret:
            mock_super_conn.side_effect = [OperationalError("boom"), fresh_connection]
            mock_get_secret.return_value = {
                "username": "live-user",
                "password": "new-pass",
            }

            result = wrapper.get_new_connection({"user": "cached", "password": "cached"})

        self.assertIs(result, fresh_connection)
        mock_get_secret.assert_called_once_with(
            "RDS_SECRET_ARN",
            region_name="us-west-2",
            force_refresh=True,
        )

        # First call uses stale credentials; second call must use the refreshed ones.
        first_call = mock_super_conn.call_args_list[0].args[0]
        second_call = mock_super_conn.call_args_list[1].args[0]
        self.assertEqual(first_call["password"], "cached")
        self.assertEqual(second_call["password"], "new-pass")

import json
import os
from unittest import TestCase
from unittest.mock import MagicMock, patch

from erieiron_public import agent_tools


class TestSecretRefresh(TestCase):
    @patch.dict(os.environ, {"AWS_DEFAULT_REGION": "us-west-2"}, clear=True)
    @patch("erieiron_public.agent_tools.time.monotonic")
    @patch("erieiron_public.agent_tools.boto3.client")
    def test_cache_refreshes_after_force_and_ttl(self, mock_boto_client, mock_monotonic):
        mock_secret_client = MagicMock()
        mock_secret_client.get_secret_value.side_effect = [
            {"SecretString": json.dumps({"password": "initial"})},
            {"SecretString": json.dumps({"password": "forced"})},
            {"SecretString": json.dumps({"password": "rotated"})},
        ]
        mock_boto_client.return_value = mock_secret_client
        mock_monotonic.side_effect = [0, 10, 20, 100]

        cache = agent_tools.SecretsManagerCache(ttl_seconds=50)

        first = cache.get_secret("arn:secret", "us-west-2")
        second = cache.get_secret("arn:secret", "us-west-2")
        forced = cache.get_secret("arn:secret", "us-west-2", force_refresh=True)
        rotated = cache.get_secret("arn:secret", "us-west-2")

        self.assertEqual(first["password"], "initial")
        self.assertEqual(second["password"], "initial")
        self.assertEqual(forced["password"], "forced")
        self.assertEqual(rotated["password"], "rotated")
        self.assertEqual(mock_secret_client.get_secret_value.call_count, 3)

    @patch.dict(
        os.environ,
        {
            "AWS_DEFAULT_REGION": "us-west-2",
            "ERIEIRON_DB_HOST": "db.example.com",
            "ERIEIRON_DB_PORT": "5432",
            "ERIEIRON_DB_NAME": "appdb",
        },
        clear=True,
    )
    @patch("erieiron_public.agent_tools.get_secret_from_env_arn")
    @patch("erieiron_public.agent_tools.pg8000.connect")
    def test_get_pg8000_connection_can_force_secret_refresh(self, mock_connect, mock_get_secret):
        class DummyConnection:
            def close(self):
                pass

        mock_connect.return_value = DummyConnection()
        mock_get_secret.return_value = {
            "username": "appuser",
            "password": "rotated-password",
            "dbname": "appdb",
        }

        conn = agent_tools.get_pg8000_connection(
            region_name="us-west-2",
            force_secret_refresh=True,
        )

        mock_get_secret.assert_called_once_with(
            "RDS_SECRET_ARN",
            "us-west-2",
            force_refresh=True,
        )
        mock_connect.assert_called_once()
        kwargs = mock_connect.call_args.kwargs
        self.assertEqual(kwargs["user"], "appuser")
        self.assertEqual(kwargs["password"], "rotated-password")
        self.assertEqual(kwargs["database"], "appdb")

        conn.close()

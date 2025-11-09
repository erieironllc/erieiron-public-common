# erieiron-public-common

to update the version, do this
```bash
export EIPC_TAG=<tag_number> && git tag -a v0.1.$EIPC_TAG -m "updating tag to $EIPC_TAG" && git push origin v0.1.$EIPC_TAG
```

## Database secret refresh

- `agent_tools.get_database_conf()` now points Django at `erieiron_public.db.backends.dynamic_postgresql`, which asks AWS Secrets Manager for the latest credentials every time a new connection is created.
- Control caching via `AWS_SECRET_CACHE_TTL_SECONDS` (defaults to 300 seconds). Set it lower than your rotation interval so credentials refresh automatically without overloading Secrets Manager.
- `DJANGO_DB_CONN_MAX_AGE` now defaults to `0` to force fresh connections. Override it with an env var if you want Django to hold onto connections for a short window.
- Utility helpers such as `get_pg8000_connection(region_name, force_secret_refresh=True)` can bypass the cache whenever you want to force an immediate refresh (e.g., troubleshooting a rotation event).

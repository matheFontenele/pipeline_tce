#!/usr/bin/env bash
set -euo pipefail

airflow db migrate

if ! airflow users list --output json | python -c '
import json, sys
username = sys.argv[1]
payload = json.load(sys.stdin)
users = payload if isinstance(payload, list) else payload.get("users", [])
raise SystemExit(0 if any(user.get("username") == username for user in users) else 1)
' "$AIRFLOW_ADMIN_USERNAME"; then
  airflow users create \
    --username "$AIRFLOW_ADMIN_USERNAME" \
    --password "$AIRFLOW_ADMIN_PASSWORD" \
    --firstname Platform \
    --lastname Administrator \
    --role Admin \
    --email "$AIRFLOW_ADMIN_EMAIL"
fi

airflow pools set tce_api_pool "${TCE_API_POOL_SLOTS:-1}" \
  "Concurrency limit for TCE public API requests"

import os
import uuid

import psycopg2
import psycopg2.extensions
import pytest

from pipeline_tce.state.bootstrap import provision
from pipeline_tce.state.config import state_dsn_from_env
from pipeline_tce.state.repository import PartitionRepository

ADMIN_DSN = os.environ.get("TCE_STATE_TEST_DSN", "").strip()
requires_postgres = pytest.mark.skipif(
    not ADMIN_DSN,
    reason="TCE_STATE_TEST_DSN não definido; teste de integração Postgres ignorado",
)


def _admin_parameters():
    return psycopg2.extensions.parse_dsn(ADMIN_DSN)


@requires_postgres
def test_provisiona_papel_banco_e_schema_de_forma_idempotente():
    base = _admin_parameters()
    suffix = uuid.uuid4().hex[:10]
    role = f"state_role_{suffix}"
    database = f"state_db_{suffix}"
    environ = {
        "TCE_STATE_DB_HOST": base.get("host", "localhost"),
        "TCE_STATE_DB_PORT": base.get("port", "5432"),
        "TCE_STATE_DB_NAME": database,
        "TCE_STATE_DB_USER": role,
        "TCE_STATE_DB_PASSWORD": "senha de teste:ç",
        "TCE_STATE_ADMIN_DB": base.get("dbname", "postgres"),
        "TCE_STATE_ADMIN_USER": base.get("user"),
        "TCE_STATE_ADMIN_PASSWORD": base.get("password"),
    }

    try:
        first = provision(environ)
        second = provision(environ)

        assert first["role_action"] == "criado"
        assert first["database_action"] == "criado"
        assert second["role_action"] == "atualizado"
        assert second["database_action"] == "existente"
        assert second["schema"] == "ok"

        repository = PartitionRepository(state_dsn_from_env(environ))
        repository.ensure_schema()
        assert repository.counts() == {}
    finally:
        admin = psycopg2.connect(ADMIN_DSN, connect_timeout=5)
        admin.autocommit = True
        try:
            with admin.cursor() as cursor:
                cursor.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
                cursor.execute(f'DROP ROLE IF EXISTS "{role}"')
        finally:
            admin.close()

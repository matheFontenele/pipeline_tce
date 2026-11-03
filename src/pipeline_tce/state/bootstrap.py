"""Provisionamento idempotente do banco de estado da plataforma.

Cria papel e banco dedicados no Postgres já existente (sem novo container) e
aplica o schema. Pode ser executado repetidamente: senha e schema convergem
para a configuração atual. Nunca imprime credenciais.
"""

from __future__ import annotations

import logging
from typing import Mapping

from pipeline_tce.state.config import (
    admin_dsn_from_env,
    state_dsn_from_env,
    state_settings_from_env,
)
from pipeline_tce.state.errors import StateStoreError
from pipeline_tce.state.repository import PartitionRepository

log = logging.getLogger(__name__)


def provision(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Garante papel, banco e schema do estado; retorna um resumo sem segredos."""
    settings = state_settings_from_env(environ)
    admin_dsn = admin_dsn_from_env(environ)

    try:
        import psycopg2
        from psycopg2 import sql
    except ImportError as exc:  # pragma: no cover - depende da imagem
        raise StateStoreError(
            "psycopg2 não está disponível na imagem; instale a dependência"
        ) from exc

    try:
        connection = psycopg2.connect(admin_dsn, connect_timeout=5)
    except psycopg2.Error:
        raise StateStoreError(
            "Não foi possível conectar como administrador para provisionar o "
            "banco de estado; confira as credenciais administrativas"
        ) from None

    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (settings.user,)
            )
            if cursor.fetchone():
                cursor.execute(
                    sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD %s").format(
                        sql.Identifier(settings.user)
                    ),
                    (settings.password,),
                )
                role_action = "atualizado"
            else:
                cursor.execute(
                    sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD %s").format(
                        sql.Identifier(settings.user)
                    ),
                    (settings.password,),
                )
                role_action = "criado"

            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (settings.database,),
            )
            if cursor.fetchone():
                database_action = "existente"
            else:
                cursor.execute(
                    sql.SQL("CREATE DATABASE {} OWNER {}").format(
                        sql.Identifier(settings.database),
                        sql.Identifier(settings.user),
                    )
                )
                database_action = "criado"
    finally:
        connection.close()

    PartitionRepository(state_dsn_from_env(environ)).ensure_schema()
    return {
        "role": settings.user,
        "role_action": role_action,
        "database": settings.database,
        "database_action": database_action,
        "schema": "ok",
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        summary = provision()
    except StateStoreError as exc:
        log.error("%s", exc)
        return 1
    log.info(
        "Banco de estado pronto: papel=%s (%s), banco=%s (%s), schema=%s",
        summary["role"],
        summary["role_action"],
        summary["database"],
        summary["database_action"],
        summary["schema"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Resolução de configuração do banco de estado.

O DSN pode vir pronto (``TCE_STATE_DSN``) ou ser montado a partir de partes.
Montar por partes evita credencial padrão no Compose e permite ``make_dsn``
escapar senhas corretamente.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Mapping

from pipeline_tce.state.errors import StateStoreError

STATE_DSN_VARIABLE = "TCE_STATE_DSN"
DEFAULT_HOST = "airflow-postgres"
DEFAULT_PORT = "5432"
DEFAULT_DATABASE = "pipeline_state"
DEFAULT_USER = "pipeline_state"
ADMIN_DATABASE = "postgres"


def _values(environ: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if environ is None else environ


@dataclass(frozen=True)
class StateSettings:
    host: str
    port: str
    database: str
    user: str
    password: str


def _make_dsn(**parameters: str) -> str:
    try:
        from psycopg2.extensions import make_dsn
    except ImportError as exc:  # pragma: no cover - depende da imagem
        raise StateStoreError(
            "psycopg2 não está disponível na imagem; instale a dependência"
        ) from exc
    return make_dsn(**parameters)


def state_settings_from_env(
    environ: Mapping[str, str] | None = None,
) -> StateSettings:
    values = _values(environ)
    password = values.get("TCE_STATE_DB_PASSWORD", "")
    if not password:
        raise StateStoreError(
            "TCE_STATE_DB_PASSWORD não configurado; defina a senha do papel "
            f"{values.get('TCE_STATE_DB_USER', DEFAULT_USER)} para habilitar o estado"
        )
    return StateSettings(
        host=values.get("TCE_STATE_DB_HOST", DEFAULT_HOST),
        port=values.get("TCE_STATE_DB_PORT", DEFAULT_PORT),
        database=values.get("TCE_STATE_DB_NAME", DEFAULT_DATABASE),
        user=values.get("TCE_STATE_DB_USER", DEFAULT_USER),
        password=password,
    )


def state_dsn_from_env(environ: Mapping[str, str] | None = None) -> str:
    """DSN da aplicação: explícito tem precedência sobre as partes."""
    values = _values(environ)
    explicit = values.get(STATE_DSN_VARIABLE, "").strip()
    if explicit:
        return explicit
    settings = state_settings_from_env(values)
    return _make_dsn(
        host=settings.host,
        port=settings.port,
        dbname=settings.database,
        user=settings.user,
        password=settings.password,
    )


def admin_dsn_from_env(environ: Mapping[str, str] | None = None) -> str:
    """DSN administrativo usado apenas no provisionamento idempotente."""
    values = _values(environ)
    user = values.get("TCE_STATE_ADMIN_USER", "").strip()
    password = values.get("TCE_STATE_ADMIN_PASSWORD", "")
    if not user or not password:
        raise StateStoreError(
            "Credenciais administrativas ausentes para provisionar o banco de "
            "estado (TCE_STATE_ADMIN_USER/TCE_STATE_ADMIN_PASSWORD)"
        )
    return _make_dsn(
        host=values.get("TCE_STATE_DB_HOST", DEFAULT_HOST),
        port=values.get("TCE_STATE_DB_PORT", DEFAULT_PORT),
        dbname=values.get("TCE_STATE_ADMIN_DB", ADMIN_DATABASE),
        user=user,
        password=password,
    )

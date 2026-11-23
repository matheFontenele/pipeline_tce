"""Repositório Postgres do estado de ingestão por partição."""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from pipeline_tce.state.config import state_dsn_from_env
from pipeline_tce.state.errors import StateStoreError, UnknownPartitionError
from pipeline_tce.state.models import (
    InvalidTransitionError,
    PartitionKey,
    PartitionSnapshot,
    PartitionStatus,
    ensure_transition,
)

log = logging.getLogger(__name__)

SCHEMA_PATH = Path(__file__).with_name("schema.sql")
MAX_ERROR_LENGTH = 1000

_COLUMNS = (
    "endpoint_prefix, year, month, municipality_code, status, attempts, "
    "row_count, content_sha256, schema_version, storage_uri, last_run_id, "
    "last_error, created_at, updated_at, extracted_at, published_at"
)

_KEY_PREDICATE = (
    "endpoint_prefix = %(endpoint_prefix)s "
    "AND year = %(year)s "
    "AND month = %(month)s "
    "AND municipality_code = %(municipality_code)s"
)

_ASSIGNABLE = frozenset(
    {
        "row_count",
        "content_sha256",
        "schema_version",
        "storage_uri",
        "last_run_id",
        "last_error",
        "extracted_at",
        "published_at",
    }
)


def _key_params(key: PartitionKey) -> dict[str, Any]:
    return {
        "endpoint_prefix": key.endpoint_prefix,
        "year": key.year,
        "month": key.storage_month,
        "municipality_code": key.municipality_code,
    }


def _snapshot(row: Mapping[str, Any]) -> PartitionSnapshot:
    key = PartitionKey(
        endpoint_prefix=row["endpoint_prefix"],
        year=row["year"],
        month=row["month"] or None,
        municipality_code=row["municipality_code"],
    )
    return PartitionSnapshot(
        key=key,
        status=PartitionStatus(row["status"]),
        attempts=row["attempts"],
        row_count=row["row_count"],
        content_sha256=row["content_sha256"],
        schema_version=row["schema_version"],
        storage_uri=row["storage_uri"],
        last_run_id=row["last_run_id"],
        last_error=row["last_error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        extracted_at=row["extracted_at"],
        published_at=row["published_at"],
    )


class PartitionRepository:
    """Acesso transacional à tabela ``ingestion_partitions``.

    Cada método abre sua própria conexão e transação. O uso pretendido é uma
    instância por task do Airflow, não um pool compartilhado entre threads.
    """

    def __init__(self, dsn: str) -> None:
        if not dsn or not dsn.strip():
            raise ValueError("DSN do banco de estado não pode ser vazio")
        self._dsn = dsn

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "PartitionRepository":
        return cls(state_dsn_from_env(environ))

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        try:
            import psycopg2
        except ImportError as exc:  # pragma: no cover - depende da imagem
            raise StateStoreError(
                "psycopg2 não está disponível na imagem; instale a dependência"
            ) from exc
        try:
            connection = psycopg2.connect(self._dsn, connect_timeout=5)
        except psycopg2.Error:
            raise StateStoreError(
                "Não foi possível conectar ao banco de estado; confira host, "
                "credenciais e disponibilidade"
            ) from None
        try:
            yield connection
        finally:
            connection.close()

    def ensure_schema(self) -> None:
        """Cria a tabela e índices de forma idempotente e concorrente-segura."""
        statement = SCHEMA_PATH.read_text(encoding="utf-8")
        with self._connect() as connection, connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtext('pipeline_tce.state.schema'))"
                )
                cursor.execute(statement)

    def get(self, key: PartitionKey) -> PartitionSnapshot | None:
        from psycopg2.extras import RealDictCursor

        with self._connect() as connection, connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"SELECT {_COLUMNS} FROM ingestion_partitions "
                    f"WHERE {_KEY_PREDICATE}",
                    _key_params(key),
                )
                row = cursor.fetchone()
        return _snapshot(row) if row else None

    def ensure(self, key: PartitionKey) -> PartitionSnapshot:
        """Registra a partição como pendente se ainda não existir.

        Não rebaixa estado existente: descoberta recorrente não deve forçar
        reprocessamento de partição já publicada.
        """
        from psycopg2.extras import RealDictCursor

        params = _key_params(key)
        with self._connect() as connection, connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    "INSERT INTO ingestion_partitions "
                    "(endpoint_prefix, year, month, municipality_code, status) "
                    "VALUES (%(endpoint_prefix)s, %(year)s, %(month)s, "
                    "%(municipality_code)s, 'pending') "
                    "ON CONFLICT (endpoint_prefix, year, municipality_code, month) "
                    "DO NOTHING "
                    f"RETURNING {_COLUMNS}",
                    params,
                )
                row = cursor.fetchone()
                if row is None:
                    cursor.execute(
                        f"SELECT {_COLUMNS} FROM ingestion_partitions "
                        f"WHERE {_KEY_PREDICATE}",
                        params,
                    )
                    row = cursor.fetchone()
        assert row is not None
        return _snapshot(row)

    def request_reprocess(self, key: PartitionKey) -> PartitionSnapshot:
        """Solicita explicitamente nova extração de uma partição.

        Permitido a partir de ``extracted``, ``validated``, ``published``,
        ``no_data`` e ``failed``; a próxima execução reextrai e sobrescreve o
        Parquet de staging/bronze apenas após nova resposta completa.
        """
        return self._transition(key, PartitionStatus.PENDING, last_error=None)

    def mark_running(self, key: PartitionKey, run_id: str) -> PartitionSnapshot:
        if not run_id:
            raise ValueError("run_id é obrigatório")
        return self._transition(
            key,
            PartitionStatus.RUNNING,
            attempts_increment=True,
            last_run_id=run_id,
            last_error=None,
        )

    def mark_extracted(
        self,
        key: PartitionKey,
        *,
        run_id: str,
        row_count: int,
        content_sha256: str,
        schema_version: str,
        storage_uri: str,
    ) -> PartitionSnapshot:
        if row_count < 0:
            raise ValueError("row_count não pode ser negativo")
        if len(content_sha256) != 64:
            raise ValueError("content_sha256 deve ter 64 caracteres hexadecimais")
        return self._transition(
            key,
            PartitionStatus.EXTRACTED,
            last_run_id=run_id,
            row_count=row_count,
            content_sha256=content_sha256,
            schema_version=schema_version,
            storage_uri=storage_uri,
            extracted_at=_now(),
        )

    def mark_no_data(self, key: PartitionKey, *, run_id: str) -> PartitionSnapshot:
        return self._transition(
            key,
            PartitionStatus.NO_DATA,
            last_run_id=run_id,
            row_count=0,
        )

    def mark_validated(
        self, key: PartitionKey, *, run_id: str, schema_version: str
    ) -> PartitionSnapshot:
        return self._transition(
            key,
            PartitionStatus.VALIDATED,
            last_run_id=run_id,
            schema_version=schema_version,
        )

    def mark_published(
        self, key: PartitionKey, *, run_id: str, storage_uri: str
    ) -> PartitionSnapshot:
        return self._transition(
            key,
            PartitionStatus.PUBLISHED,
            last_run_id=run_id,
            storage_uri=storage_uri,
            published_at=_now(),
        )

    def mark_failed(
        self, key: PartitionKey, *, run_id: str, error: str
    ) -> PartitionSnapshot:
        return self._transition(
            key,
            PartitionStatus.FAILED,
            last_run_id=run_id,
            last_error=_truncate(error),
        )

    def pending(self, limit: int = 1) -> list[PartitionSnapshot]:
        if limit <= 0:
            return []
        from psycopg2.extras import RealDictCursor

        with self._connect() as connection, connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"SELECT {_COLUMNS} FROM ingestion_partitions "
                    "WHERE status = 'pending' "
                    "ORDER BY endpoint_prefix, year, month, municipality_code "
                    "LIMIT %s",
                    (limit,),
                )
                rows = cursor.fetchall()
        return [_snapshot(row) for row in rows]

    def counts(self) -> dict[str, int]:
        with self._connect() as connection, connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT status, count(*) FROM ingestion_partitions GROUP BY status"
                )
                rows = cursor.fetchall()
        return {status: int(total) for status, total in rows}

    def _transition(
        self,
        key: PartitionKey,
        target: PartitionStatus,
        *,
        attempts_increment: bool = False,
        **assignments: Any,
    ) -> PartitionSnapshot:
        unknown = set(assignments) - _ASSIGNABLE
        if unknown:
            raise ValueError(f"Campos não atualizáveis: {sorted(unknown)}")

        from psycopg2.extras import RealDictCursor

        params = _key_params(key)
        params["target_status"] = target.value
        sets = ["status = %(target_status)s", "updated_at = now()"]
        if attempts_increment:
            sets.append("attempts = attempts + 1")
        for name, value in assignments.items():
            sets.append(f"{name} = %({name})s")
            params[name] = value

        with self._connect() as connection, connection:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    f"SELECT {_COLUMNS} FROM ingestion_partitions "
                    f"WHERE {_KEY_PREDICATE} FOR UPDATE",
                    _key_params(key),
                )
                current = cursor.fetchone()
                if current is None:
                    raise UnknownPartitionError(
                        "Partição não registrada no estado: "
                        f"{key.endpoint_prefix}/{key.year}/{key.storage_month}/"
                        f"{key.municipality_code}"
                    )
                current_status = PartitionStatus(current["status"])

                if current_status is target:
                    return _snapshot(current)
                try:
                    ensure_transition(current_status, target)
                except InvalidTransitionError:
                    log.warning(
                        "Transição recusada; endpoint=%s arquivo=%s de=%s para=%s",
                        key.endpoint_prefix,
                        key.filename,
                        current_status.value,
                        target.value,
                    )
                    raise

                cursor.execute(
                    f"UPDATE ingestion_partitions SET {', '.join(sets)} "
                    f"WHERE {_KEY_PREDICATE} RETURNING {_COLUMNS}",
                    params,
                )
                updated = cursor.fetchone()
        assert updated is not None
        return _snapshot(updated)


def _truncate(message: str) -> str:
    text = " ".join((message or "").split())
    if len(text) <= MAX_ERROR_LENGTH:
        return text
    return text[: MAX_ERROR_LENGTH - 1] + "…"


def _now() -> datetime:
    return datetime.now(timezone.utc)

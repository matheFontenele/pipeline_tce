"""Cliente HTTP resiliente e extração isolada de uma partição do TCE-CE."""

from __future__ import annotations

import logging
import math
import os
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as parquet
import requests

from pipeline_tce.ingestion.planning import Partition

log = logging.getLogger(__name__)


class TceApiError(RuntimeError):
    """Erro sanitizado da API; não inclui corpo/resposta nem query string."""


class TceRateLimitError(TceApiError):
    """A API continuou limitando chamadas após o orçamento de tentativas."""


class PilotStorageLimitError(TceApiError):
    """O volume isolado atingiu o limite preventivo de ocupação."""


@dataclass(frozen=True)
class RetryPolicy:
    timeout_seconds: int = 90
    max_attempts: int = 5
    http_400_attempts: int = 3
    min_interval_seconds: float = 1.5
    http_400_backoff_seconds: float = 60.0
    http_429_backoff_seconds: float = 30.0
    connection_backoff_seconds: float = 15.0
    max_backoff_seconds: float = 300.0
    max_retry_after_seconds: float = 900.0

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0 or self.max_attempts <= 0 or self.http_400_attempts <= 0:
            raise ValueError("Timeout e limites de tentativas devem ser positivos")
        delays = (
            self.min_interval_seconds,
            self.http_400_backoff_seconds,
            self.http_429_backoff_seconds,
            self.connection_backoff_seconds,
            self.max_backoff_seconds,
        )
        if any(not math.isfinite(delay) or delay < 0 for delay in delays):
            raise ValueError("Intervalos e backoffs devem ser valores finitos e não negativos")
        if not math.isfinite(self.max_retry_after_seconds) or self.max_retry_after_seconds <= 0:
            raise ValueError("Intervalos e backoffs devem ser não negativos")


def retry_policy_from_env(environ: dict[str, str] | None = None) -> RetryPolicy:
    values = os.environ if environ is None else environ
    return RetryPolicy(
        timeout_seconds=int(values.get("TCE_HTTP_TIMEOUT", "90")),
        max_attempts=int(values.get("TCE_HTTP_MAX_ATTEMPTS", "5")),
        http_400_attempts=int(values.get("TCE_HTTP_400_RETRIES", "3")),
        min_interval_seconds=float(values.get("TCE_API_MIN_INTERVAL_SECONDS", "1.5")),
        http_400_backoff_seconds=float(values.get("TCE_HTTP_400_BACKOFF_SECONDS", "60")),
        http_429_backoff_seconds=float(values.get("TCE_HTTP_429_BACKOFF_SECONDS", "30")),
        connection_backoff_seconds=float(values.get("TCE_HTTP_CONNECTION_BACKOFF_SECONDS", "15")),
        max_backoff_seconds=float(values.get("TCE_HTTP_MAX_BACKOFF_SECONDS", "300")),
        max_retry_after_seconds=float(values.get("TCE_HTTP_MAX_RETRY_AFTER_SECONDS", "900")),
    )


def pilot_storage_limit_from_env(environ: dict[str, str] | None = None) -> float:
    values = os.environ if environ is None else environ
    try:
        limit = float(values.get("TCE_PILOT_MAX_DISK_USAGE", "0.90"))
    except (TypeError, ValueError) as exc:
        raise ValueError("TCE_PILOT_MAX_DISK_USAGE deve ser uma fração entre 0 e 1") from exc
    if not math.isfinite(limit) or not 0 < limit <= 1:
        raise ValueError("TCE_PILOT_MAX_DISK_USAGE deve ser uma fração entre 0 e 1")
    return limit


def ensure_pilot_storage_below_limit(
    output_dir: Path, max_usage: float | None = None
) -> float:
    """Aborta antes de trabalhar se o filesystem do volume atingiu o limite."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    limit = pilot_storage_limit_from_env() if max_usage is None else max_usage
    if not math.isfinite(limit) or not 0 < limit <= 1:
        raise ValueError("O limite de ocupação deve ser uma fração entre 0 e 1")
    usage = shutil.disk_usage(output_dir)
    ratio = usage.used / usage.total if usage.total else 1.0
    if ratio >= limit:
        raise PilotStorageLimitError(
            "Limite preventivo do volume piloto atingido: "
            f"ocupação={ratio:.1%}, limite={limit:.1%}; lote interrompido"
        )
    return ratio


class RequestPacer:
    """Serializa e espaça requests dentro do processo atual."""

    def __init__(
        self,
        min_interval_seconds: float,
        *,
        clock=time.monotonic,
        sleep=time.sleep,
    ) -> None:
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds não pode ser negativo")
        self._interval = min_interval_seconds
        self._clock = clock
        self._sleep = sleep
        self._next_allowed = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            delay = max(0.0, self._next_allowed - self._clock())
            if delay:
                self._sleep(delay)
            self._next_allowed = self._clock() + self._interval

    def pause(self, seconds: float) -> None:
        with self._lock:
            self._next_allowed = max(self._next_allowed, self._clock() + max(0.0, seconds))


def _retry_after_seconds(value: str | None, fallback: float, maximum: float) -> float:
    try:
        parsed = float(value) if value is not None else fallback
    except (TypeError, ValueError):
        parsed = fallback
    if not math.isfinite(parsed):
        parsed = fallback
    return min(maximum, max(1.0, parsed))


def _fetch_page(
    session: requests.Session,
    partition: Partition,
    params: dict[str, int | str],
    policy: RetryPolicy,
    pacer: RequestPacer,
) -> list[dict[str, Any]]:
    attempts = max(policy.max_attempts, policy.http_400_attempts)
    for attempt in range(1, attempts + 1):
        pacer.wait()
        try:
            response = session.get(
                partition.endpoint.url,
                params=params,
                headers={"Accept": "application/json", "User-Agent": "pipeline-tce/0.1"},
                timeout=policy.timeout_seconds,
            )
        except requests.exceptions.RequestException as exc:
            if attempt >= policy.max_attempts:
                raise TceApiError(
                    f"Falha de conexão após {attempt} tentativas: {type(exc).__name__}"
                ) from exc
            delay = min(
                policy.max_backoff_seconds,
                policy.connection_backoff_seconds * (2 ** (attempt - 1)),
            )
            log.warning(
                "API TCE indisponível; endpoint=%s partição=%s tentativa=%s/%s espera=%.0fs",
                partition.endpoint.prefix,
                partition.filename,
                attempt,
                policy.max_attempts,
                delay,
            )
            pacer.pause(delay)
            continue

        if response.status_code == 429:
            delay = _retry_after_seconds(
                response.headers.get("Retry-After"),
                min(policy.max_retry_after_seconds, policy.http_429_backoff_seconds * (2 ** (attempt - 1))),
                policy.max_retry_after_seconds,
            )
            log.warning(
                "API TCE limitou chamadas; endpoint=%s partição=%s tentativa=%s/%s espera=%.0fs",
                partition.endpoint.prefix,
                partition.filename,
                attempt,
                policy.max_attempts,
                delay,
            )
            if attempt >= policy.max_attempts:
                raise TceRateLimitError(
                    f"HTTP 429 persistiu após {attempt} tentativas para {partition.filename}"
                )
            pacer.pause(delay)
            continue

        if response.status_code == 400:
            delay = min(
                policy.max_backoff_seconds,
                policy.http_400_backoff_seconds * (2 ** (attempt - 1)),
            )
            log.warning(
                "API TCE retornou HTTP 400; endpoint=%s partição=%s tentativa=%s/%s",
                partition.endpoint.prefix,
                partition.filename,
                attempt,
                policy.http_400_attempts,
            )
            if attempt >= policy.http_400_attempts:
                raise TceApiError(
                    f"HTTP 400 após {attempt} tentativas para {partition.filename}"
                )
            pacer.pause(delay)
            continue

        if response.status_code >= 500:
            if attempt >= policy.max_attempts:
                raise TceApiError(
                    f"HTTP {response.status_code} após {attempt} tentativas para {partition.filename}"
                )
            delay = min(
                policy.max_backoff_seconds,
                policy.connection_backoff_seconds * (2 ** (attempt - 1)),
            )
            log.warning(
                "API TCE retornou HTTP %s; endpoint=%s partição=%s tentativa=%s/%s",
                response.status_code,
                partition.endpoint.prefix,
                partition.filename,
                attempt,
                policy.max_attempts,
            )
            pacer.pause(delay)
            continue

        if response.status_code != 200:
            raise TceApiError(
                f"HTTP {response.status_code} para {partition.filename}; erro não retentável"
            )

        try:
            payload = response.json()
        except (ValueError, requests.exceptions.JSONDecodeError) as exc:
            raise TceApiError(f"JSON inválido recebido para {partition.filename}") from exc
        elements = payload.get("elements") if isinstance(payload, dict) else None
        if not isinstance(elements, list):
            raise TceApiError(f"Resposta sem lista 'elements' para {partition.filename}")
        if any(not isinstance(row, dict) for row in elements):
            raise TceApiError(f"Resposta contém registro não estruturado em {partition.filename}")
        return elements

    raise TceApiError(f"Tentativas esgotadas para {partition.filename}")


def _valid_parquet(path: Path) -> bool:
    try:
        parquet.ParquetFile(path).metadata
        return True
    except Exception:
        return False


def _write_parquet_atomic(frame: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent, delete=False
        ) as temporary:
            temporary_name = temporary.name
        frame.to_parquet(temporary_name, engine="pyarrow", compression="snappy", index=False)
        os.replace(temporary_name, destination)
    finally:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)


def extract_partition(
    partition: Partition,
    output_dir: Path,
    *,
    session: requests.Session | None = None,
    policy: RetryPolicy | None = None,
    pacer: RequestPacer | None = None,
    today: date | None = None,
    max_disk_usage: float | None = None,
) -> dict[str, Any]:
    """Extrai uma partição e escreve atomicamente no destino isolado informado."""
    output_dir = Path(output_dir).resolve()
    destination = output_dir / partition.filename
    if destination.exists() and _valid_parquet(destination):
        metadata = parquet.ParquetFile(destination).metadata
        status = "no_data_cached" if metadata.num_columns == 0 else "already_present"
        log.info("Partição já existe e é legível; endpoint=%s arquivo=%s", partition.endpoint.prefix, destination.name)
        return {"status": status, "file": destination.name, "rows": metadata.num_rows}
    if destination.exists():
        log.warning("Parquet inválido será recuperado; endpoint=%s arquivo=%s", partition.endpoint.prefix, destination.name)

    ensure_pilot_storage_below_limit(output_dir, max_disk_usage)
    policy = policy or RetryPolicy()
    pacer = pacer or RequestPacer(policy.min_interval_seconds)
    own_session = session is None
    session = session or requests.Session()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    writer: parquet.ParquetWriter | None = None
    schema: pa.Schema | None = None
    rows_written = 0
    page_number = 0
    latest_offset = 0
    try:
        params = dict(partition.params)
        page_size = int(params["$count"])
        offset = int(params["$start_index"])
        with tempfile.NamedTemporaryFile(
            prefix=f".{destination.name}.",
            suffix=".tmp",
            dir=destination.parent,
            delete=False,
        ) as temporary:
            temporary_name = temporary.name

        while True:
            ensure_pilot_storage_below_limit(output_dir, max_disk_usage)
            params["$start_index"] = offset
            rows = _fetch_page(session, partition, params, policy, pacer)
            page_number += 1
            latest_offset = offset
            page_frame = pd.DataFrame(rows)
            if not page_frame.empty and "exercicio_orcamento" in page_frame.columns:
                expected_year = str(partition.year)
                years = page_frame["exercicio_orcamento"].astype(str).str[:4]
                page_frame = page_frame.loc[years == expected_year].copy()

            if not page_frame.empty:
                page_frame["municipio_referencia"] = partition.municipality.name
                table = pa.Table.from_pandas(page_frame, preserve_index=False)
                if schema is None:
                    schema = table.schema
                    writer = parquet.ParquetWriter(
                        temporary_name,
                        schema,
                        compression="snappy",
                    )
                else:
                    extra_columns = set(table.column_names) - set(schema.names)
                    if extra_columns:
                        raise TceApiError(
                            "Schema mudou entre páginas para "
                            f"{partition.filename}: {sorted(extra_columns)}"
                        )
                    arrays = []
                    for field in schema:
                        if field.name not in table.column_names:
                            arrays.append(pa.nulls(table.num_rows, type=field.type))
                            continue
                        column = table[field.name]
                        if column.type != field.type:
                            try:
                                column = column.cast(field.type, safe=True)
                            except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                                raise TceApiError(
                                    "Tipo de coluna mudou entre páginas para "
                                    f"{partition.filename}: {field.name}"
                                ) from exc
                        arrays.append(column)
                    table = pa.Table.from_arrays(arrays, schema=schema)

                assert writer is not None
                ensure_pilot_storage_below_limit(output_dir, max_disk_usage)
                writer.write_table(table)
                rows_written += table.num_rows
                log.info(
                    "Página processada; endpoint=%s arquivo=%s página=%s offset=%s "
                    "registros_página=%s registros_acumulados=%s",
                    partition.endpoint.prefix,
                    partition.filename,
                    page_number,
                    offset,
                    table.num_rows,
                    rows_written,
                )

            if len(rows) < page_size:
                break
            offset += page_size
    except Exception:
        if writer is not None:
            writer.close()
            writer = None
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)
        raise
    finally:
        if writer is not None:
            writer.close()
        if own_session:
            session.close()

    current_year = (today or date.today()).year
    if rows_written == 0:
        if temporary_name and os.path.exists(temporary_name):
            os.unlink(temporary_name)
        if partition.year < current_year:
            # Semântica herdada: zero colunas é cache negativo intencional.
            ensure_pilot_storage_below_limit(output_dir, max_disk_usage)
            _write_parquet_atomic(pd.DataFrame(), destination)
            status = "no_data_cached"
        else:
            status = "no_data_uncached"
        result = {"status": status, "file": destination.name, "rows": 0}
    else:
        assert temporary_name is not None
        try:
            ensure_pilot_storage_below_limit(output_dir, max_disk_usage)
        except Exception:
            if os.path.exists(temporary_name):
                os.unlink(temporary_name)
            raise
        os.replace(temporary_name, destination)
        result = {"status": "downloaded", "file": destination.name, "rows": rows_written}

    log.info(
        "Partição concluída; endpoint=%s arquivo=%s status=%s linhas=%s",
        partition.endpoint.prefix,
        result["file"],
        result["status"],
        result["rows"],
    )
    return result

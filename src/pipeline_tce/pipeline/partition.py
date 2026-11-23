"""Ciclo de uma partição com estado, storage e manifesto."""

from __future__ import annotations

import json
import logging
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from pipeline_tce.ingestion.client import (
    RequestPacer,
    RetryPolicy,
    extract_partition,
)
from pipeline_tce.ingestion.planning import Partition
from pipeline_tce.state.models import PartitionKey, PartitionStatus
from pipeline_tce.storage.base import StorageBackend
from pipeline_tce.storage.manifest import (
    build_manifest,
    manifest_key,
    write_manifest,
)

log = logging.getLogger(__name__)

SCHEMA_VERSION = "1"


class PipelineError(RuntimeError):
    """Falha controlada do ciclo da partição, sem dados sensíveis."""


class PendingDataError(PipelineError):
    """Ausência de dados no ano corrente, que não pode ser cacheada.

    Não é falha do pipeline: o estado fica ``failed`` só para ser retomado na
    próxima execução e a janela de atualização conta como "sem dados", sem
    abrir o circuit breaker.
    """


@dataclass(frozen=True)
class PartitionRunResult:
    key: PartitionKey
    status: str
    rows: int
    storage_uri: str | None
    manifest_uri: str | None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def storage_key(key: PartitionKey) -> str:
    """Layout bronze: um prefixo por endpoint e ano, arquivo plano por partição."""
    return f"bronze/{key.endpoint_prefix}/{key.year}/{key.filename}"


def run_partition(
    partition: Partition,
    *,
    repository: Any,
    storage: StorageBackend,
    run_id: str,
    staging_dir: Path,
    extract: Callable[..., dict[str, Any]] = extract_partition,
    policy: RetryPolicy | None = None,
    pacer: RequestPacer | None = None,
    max_disk_usage: float | None = None,
    refresh: bool = False,
) -> PartitionRunResult:
    """Executa extração -> storage -> manifesto -> estado para uma partição.

    Partições com artefato já íntegro (``extracted``, ``validated``,
    ``published``) e ausência histórica (``no_data``) não são reextraídas:
    só voltam a ``pending`` após ``request_reprocess`` explícito. Qualquer
    falha marca ``failed`` e propaga, sem publicar arquivo parcial. Se o
    estado indicar artefato e o storage tiver divergido, a task de conferência
    falha e o reprocessamento deve ser solicitado.

    ``refresh=True`` é o pedido explícito da janela de atualização recorrente
    (mês corrente e anteriores): a partição volta a ``pending`` e é reextraída.
    O Parquet válido só é substituído por ``put_file_atomic`` depois de uma
    resposta completa, então uma falha no meio mantém a versão anterior.
    """
    if not run_id:
        raise ValueError("run_id é obrigatório")

    key = PartitionKey.from_partition(partition)
    repository.ensure(key)
    snapshot = repository.get(key)
    if snapshot is None:
        raise PipelineError("Partição não encontrada no estado após o registro")

    if snapshot.artifact_ready or snapshot.settled:
        if not refresh:
            log.info(
                "Partição já possui artefato; reprocessamento não solicitado; arquivo=%s estado=%s",
                key.filename,
                snapshot.status.value,
            )
            return PartitionRunResult(
                key=key,
                status="skipped",
                rows=snapshot.row_count or 0,
                storage_uri=snapshot.storage_uri,
                manifest_uri=None,
            )
        log.info(
            "Atualização da janela; reextraindo partição com artefato; arquivo=%s estado=%s",
            key.filename,
            snapshot.status.value,
        )
        repository.request_reprocess(key)

    repository.mark_running(key, run_id)
    staging = Path(staging_dir)
    staging.mkdir(parents=True, exist_ok=True)

    try:
        with tempfile.TemporaryDirectory(prefix=".particao-", dir=staging) as directory:
            result = extract(
                partition,
                Path(directory),
                policy=policy,
                pacer=pacer,
                max_disk_usage=max_disk_usage,
            )
            if result.get("status") == "no_data_uncached":
                raise PendingDataError(
                    "Ausência de dados no ano corrente não é cacheada; "
                    "a partição não será publicada"
                )

            source = Path(directory) / partition.filename
            if not source.is_file():
                raise PipelineError(
                    f"Extração não produziu arquivo para {partition.filename}"
                )

            key_name = storage_key(key)
            storage_uri = storage.put_file_atomic(source, key_name)
            manifest = build_manifest(
                source,
                key=key,
                run_id=run_id,
                schema_version=SCHEMA_VERSION,
            )
            manifest_uri = write_manifest(
                storage, key=key, manifest=manifest, parquet_key=key_name
            )

            rows = int(manifest["row_count"])
            if rows == 0:
                repository.mark_no_data(key, run_id=run_id)
                final_status = "no_data"
            else:
                repository.mark_extracted(
                    key,
                    run_id=run_id,
                    row_count=rows,
                    content_sha256=manifest["content_sha256"],
                    schema_version=SCHEMA_VERSION,
                    storage_uri=storage_uri,
                )
                final_status = "extracted"

        log.info(
            "Partição concluída; arquivo=%s estado=%s linhas=%s sha256=%s",
            key.filename,
            final_status,
            rows,
            manifest["content_sha256"][:12],
        )
        return PartitionRunResult(
            key=key,
            status=final_status,
            rows=rows,
            storage_uri=storage_uri,
            manifest_uri=manifest_uri,
        )
    except Exception as exc:
        try:
            repository.mark_failed(
                key, run_id=run_id, error=f"{type(exc).__name__}: {exc}"
            )
        except Exception:  # noqa: BLE001 - preserva a exceção original
            log.exception("Não foi possível registrar 'failed' para %s", key.filename)
        raise


def verify_partition(
    key: PartitionKey, *, repository: Any, storage: StorageBackend
) -> dict[str, Any]:
    """Confere que estado, manifesto e arquivo armazenado concordam.

    A leitura usa apenas o contrato ``StorageBackend`` (``read_bytes`` e
    ``sha256``), sem caminho físico local, para valer também em S3/MinIO.
    """
    snapshot = repository.get(key)
    if snapshot is None:
        raise PipelineError("Partição não registrada no estado")
    if snapshot.status is PartitionStatus.NO_DATA:
        return {"status": "no_data", "rows": 0, "sha256": None}
    if snapshot.status not in (
        PartitionStatus.EXTRACTED,
        PartitionStatus.VALIDATED,
        PartitionStatus.PUBLISHED,
    ):
        raise PipelineError(
            f"Estado inesperado para conferência: {snapshot.status.value}"
        )

    key_name = storage_key(key)
    try:
        manifest = json.loads(storage.read_bytes(manifest_key(key_name)))
    except ValueError as exc:
        raise PipelineError("Manifesto ilegível para a partição") from exc
    actual = storage.sha256(key_name)
    if actual != manifest.get("content_sha256"):
        raise PipelineError("Arquivo armazenado diverge do manifesto")
    if actual != snapshot.content_sha256:
        raise PipelineError("Arquivo armazenado diverge do estado de ingestão")
    return {
        "status": snapshot.status.value,
        "rows": manifest["row_count"],
        "sha256": actual,
    }

"""Manifesto de partição: commit lógico independente do backend.

Em filesystem local, ``os.replace`` já é atômico. Em S3/MinIO não existe
rename atômico; o manifesto (gravado por último) é o marcador que diz que o
Parquet correspondente está completo e íntegro.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pyarrow.parquet as parquet

from pipeline_tce.state.models import PartitionKey
from pipeline_tce.storage.base import StorageBackend, StorageError

MANIFEST_FORMAT_VERSION = 1
MANIFEST_SUFFIX = ".manifest.json"


def manifest_key(filename: str) -> str:
    return f"{filename}{MANIFEST_SUFFIX}"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(
    parquet_path: Path,
    *,
    key: PartitionKey,
    run_id: str,
    schema_version: str,
    contract_version: str | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    """Descreve um Parquet concluído sem reabrir as linhas em memória."""
    if not run_id:
        raise ValueError("run_id é obrigatório para o manifesto")
    if not schema_version:
        raise ValueError("schema_version é obrigatório para o manifesto")

    path = Path(parquet_path)
    if not path.is_file():
        raise StorageError(f"Parquet inexistente para manifesto: {path.name}")

    metadata = parquet.ParquetFile(path).metadata
    return {
        "format_version": MANIFEST_FORMAT_VERSION,
        "partition": {
            "endpoint_prefix": key.endpoint_prefix,
            "year": key.year,
            "month": key.month,
            "municipality_code": key.municipality_code,
            "filename": key.filename,
        },
        "file": path.name,
        "content_sha256": file_sha256(path),
        "size_bytes": path.stat().st_size,
        "row_count": metadata.num_rows,
        "columns": list(metadata.schema.names),
        "schema_version": schema_version,
        "contract_version": contract_version,
        "run_id": run_id,
        "created_at": (created_at or datetime.now(timezone.utc)).isoformat(),
    }


def write_manifest(
    storage: StorageBackend,
    *,
    key: PartitionKey,
    manifest: Mapping[str, Any],
    parquet_key: str | None = None,
    durable: bool = True,
) -> str:
    """Grava o manifesto por último, de forma atômica no backend.

    ``durable=False`` só é usado em migração offline em lote, onde o custo de
    um fsync por arquivo domina e a conferência posterior cobre a durabilidade.
    """
    partition = manifest.get("partition")
    if not isinstance(partition, Mapping) or partition.get("filename") != key.filename:
        raise ValueError("Manifesto não corresponde à partição informada")
    payload = json.dumps(
        dict(manifest), ensure_ascii=False, indent=2, sort_keys=True
    ).encode("utf-8") + b"\n"
    return storage.put_bytes_atomic(
        payload, manifest_key(parquet_key or key.filename), fsync=durable
    )

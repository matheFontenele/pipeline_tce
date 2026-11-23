"""Adoção dos Parquets já extraídos pelo pipeline anterior.

O diretório legado é plano (``<prefixo>_<ano>[_<mes>]_<municipio>.parquet``) e o
lakehouse usa ``bronze/<prefixo>/<ano>/<arquivo>`` com manifesto e estado. Esta
migração **copia** (nunca move nem reescreve) os arquivos, gera o manifesto a
partir do próprio Parquet e registra o estado como artefato pronto, sem chamar
a API. A origem é tratada como somente leitura.

Uso dentro do container, com a origem montada em modo leitura::

    python -m pipeline_tce.migration.legacy --source /mnt/legado --dry-run
    python -m pipeline_tce.migration.legacy --source /mnt/legado --workers 12

A execução é idempotente: arquivo com manifesto íntegro e mesmo tamanho é
reaproveitado, e o estado só é gravado onde não há artefato registrado.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import shutil
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

from pipeline_tce.ingestion.catalog import (
    Endpoint,
    Municipality,
    load_endpoints,
    load_municipalities,
)
from pipeline_tce.pipeline.partition import SCHEMA_VERSION, storage_key
from pipeline_tce.state.models import PartitionKey, PartitionStatus
from pipeline_tce.storage.local import PUBLISHED_FILE_MODE, LocalStorage
from pipeline_tce.storage.manifest import build_manifest, manifest_key, write_manifest

log = logging.getLogger(__name__)

PARQUET_SUFFIX = ".parquet"
IMPORT_RUN_PREFIX = "import__"
_TAIL = re.compile(r"^(?P<year>\d{4})(?:_(?P<month>\d{2}))?_(?P<code>\d{3})$")


class LegacyNameError(ValueError):
    """Nome de arquivo fora do padrão do extrator anterior."""


@dataclass(frozen=True)
class LegacyCatalog:
    """Catálogo versionado já indexado para leitura de nomes legados."""

    endpoints: Mapping[str, Endpoint]
    municipalities: Mapping[str, Municipality]
    prefixes: tuple[str, ...]

    @classmethod
    def load(cls) -> "LegacyCatalog":
        endpoints = load_endpoints()
        return cls(
            endpoints=endpoints,
            municipalities=load_municipalities(),
            prefixes=tuple(sorted(endpoints, key=len, reverse=True)),
        )

    def planned_count(
        self,
        *,
        start_year: int,
        end_year: int,
        endpoint_prefix: str | None = None,
    ) -> dict[str, int]:
        """Universo planejado por endpoint no intervalo, sem tocar em disco."""
        totals: dict[str, int] = {}
        for prefix in sorted(self.endpoints):
            if endpoint_prefix and prefix != endpoint_prefix:
                continue
            endpoint = self.endpoints[prefix]
            months = 12 if endpoint.frequency == "mensal" else 1
            totals[prefix] = (
                months * (end_year - start_year + 1) * len(self.municipalities)
            )
        return totals


@dataclass(frozen=True)
class LegacyPartition:
    """Arquivo legado reconhecido e mapeado para uma partição do catálogo."""

    key: PartitionKey
    source: Path
    size_bytes: int
    extracted_at: datetime


def parse_filename(filename: str, catalog: LegacyCatalog) -> PartitionKey:
    """Traduz ``<prefixo>_<ano>[_<mes>]_<municipio>.parquet`` em chave de partição."""
    if not filename.endswith(PARQUET_SUFFIX):
        raise LegacyNameError("arquivo não é Parquet")
    stem = filename[: -len(PARQUET_SUFFIX)]
    for prefix in catalog.prefixes:
        if not stem.startswith(prefix + "_"):
            continue
        match = _TAIL.match(stem[len(prefix) + 1 :])
        if match is None:
            raise LegacyNameError("sufixo de ano/mês/município fora do padrão")
        endpoint = catalog.endpoints[prefix]
        month_token = match.group("month")
        if endpoint.frequency == "anual":
            if month_token:
                raise LegacyNameError("endpoint anual não aceita mês")
            month = None
        else:
            if not month_token:
                raise LegacyNameError("endpoint mensal exige mês")
            month = int(month_token)
        code = match.group("code")
        if code not in catalog.municipalities:
            raise LegacyNameError(f"município fora do catálogo: {code}")
        return PartitionKey(prefix, int(match.group("year")), month, code)
    raise LegacyNameError("prefixo de endpoint fora do catálogo")


def scan(
    source: Path | str,
    catalog: LegacyCatalog,
    *,
    endpoint_prefix: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    rejects: list[tuple[str, str]] | None = None,
) -> Iterator[LegacyPartition]:
    """Percorre a origem em ordem de diretório, sem carregar tudo em memória."""
    root = Path(source)
    if not root.is_dir():
        raise FileNotFoundError(f"Origem inexistente: {root}")
    for entry in os.scandir(root):
        if not entry.is_file():
            continue
        if not entry.name.endswith(PARQUET_SUFFIX):
            if rejects is not None:
                rejects.append((entry.name, "ignorado: não é Parquet"))
            continue
        try:
            key = parse_filename(entry.name, catalog)
        except LegacyNameError as exc:
            if rejects is not None:
                rejects.append((entry.name, str(exc)))
            continue
        if endpoint_prefix and key.endpoint_prefix != endpoint_prefix:
            continue
        if start_year is not None and key.year < start_year:
            continue
        if end_year is not None and key.year > end_year:
            continue
        stat = entry.stat()
        yield LegacyPartition(
            key=key,
            source=Path(entry.path),
            size_bytes=stat.st_size,
            extracted_at=datetime.fromtimestamp(stat.st_mtime, timezone.utc),
        )


def inventory(
    source: Path | str,
    catalog: LegacyCatalog,
    *,
    endpoint_prefix: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict[str, Any]:
    """Resume o que a origem oferece e quanto falta para o universo planejado."""
    rejects: list[tuple[str, str]] = []
    by_endpoint: Counter[str] = Counter()
    by_year: Counter[int] = Counter()
    bytes_total = 0
    total = 0
    for item in scan(
        source,
        catalog,
        endpoint_prefix=endpoint_prefix,
        start_year=start_year,
        end_year=end_year,
        rejects=rejects,
    ):
        total += 1
        bytes_total += item.size_bytes
        by_endpoint[item.key.endpoint_prefix] += 1
        by_year[item.key.year] += 1

    years = sorted(by_year)
    planned: dict[str, int] = {}
    if years:
        planned = catalog.planned_count(
            start_year=start_year or years[0],
            end_year=end_year or years[-1],
            endpoint_prefix=endpoint_prefix,
        )
    return {
        "total": total,
        "bytes": bytes_total,
        "by_endpoint": dict(sorted(by_endpoint.items())),
        "by_year": {year: by_year[year] for year in years},
        "planned": planned,
        "missing": {
            prefix: planned[prefix] - by_endpoint.get(prefix, 0)
            for prefix in planned
        },
        "rejects": rejects[:50],
        "rejects_total": len(rejects),
    }


def format_inventory(summary: Mapping[str, Any]) -> str:
    width = max([len(prefix) for prefix in summary["by_endpoint"]] + [8])
    header = f"{'endpoint':<{width}}{'arquivos':>11}{'planejadas':>12}{'faltam':>9}"
    lines = [header, "-" * len(header)]
    for prefix, count in summary["by_endpoint"].items():
        planned = summary["planned"].get(prefix, 0)
        lines.append(
            f"{prefix:<{width}}{count:>11}{planned:>12}"
            f"{planned - count:>9}"
        )
    lines.append("-" * len(header))
    planned_total = sum(summary["planned"].values())
    lines.append(
        f"{'TOTAL':<{width}}{summary['total']:>11}{planned_total:>12}"
        f"{planned_total - summary['total']:>9}"
    )
    lines.append("")
    lines.append(f"Arquivos: {summary['total']} ({summary['bytes'] / 1e9:.2f} GB)")
    years = summary["by_year"]
    if years:
        lines.append(
            f"Anos: {min(years)}–{max(years)}; "
            f"arquivos por ano: {years[min(years)]}–{years[max(years)]}"
        )
    if summary["rejects_total"]:
        lines.append(f"Rejeitados: {summary['rejects_total']}")
        for name, reason in summary["rejects"][:5]:
            lines.append(f"  {name}: {reason}")
    return "\n".join(lines)


def _reusable_manifest(path: Path, parquet: Path) -> dict[str, Any] | None:
    """Manifesto anterior ainda válido para o arquivo em disco, se houver."""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        size = parquet.stat().st_size
    except (OSError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    if int(payload.get("size_bytes", -1)) != size:
        return None
    if payload.get("schema_version") != SCHEMA_VERSION:
        return None
    sha = payload.get("content_sha256")
    if not isinstance(sha, str) or len(sha) != 64:
        return None
    return payload


def _copy_into_lakehouse(item: LegacyPartition, storage: LocalStorage, key_name: str) -> Path:
    """Copia o Parquet legado para o layout bronze, sem sobrescrever igual."""
    destination = Path(storage.uri(key_name))
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_file() and destination.stat().st_size == item.size_bytes:
        return destination
    staging = destination.with_name(
        f".{destination.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        shutil.copyfile(item.source, staging)
        os.chmod(staging, PUBLISHED_FILE_MODE)
        os.replace(staging, destination)
    finally:
        if staging.exists():
            staging.unlink()
    return destination


def _adopt_one(
    item: LegacyPartition,
    *,
    storage: LocalStorage,
    run_id: str,
) -> dict[str, Any]:
    key = item.key
    key_name = storage_key(key)
    destination = _copy_into_lakehouse(item, storage, key_name)
    manifest_path = destination.with_name(manifest_key(destination.name))

    preserved = _reusable_manifest(manifest_path, destination)
    if preserved is not None:
        manifest = preserved
    else:
        manifest = build_manifest(
            destination,
            key=key,
            run_id=run_id,
            schema_version=SCHEMA_VERSION,
            created_at=item.extracted_at,
        )
        manifest["origin"] = {
            "extractor": "legado",
            "file": item.source.name,
            "mtime": item.extracted_at.isoformat(),
        }
        write_manifest(
            storage, key=key, manifest=manifest, parquet_key=key_name, durable=False
        )

    rows = int(manifest["row_count"])
    status = PartitionStatus.NO_DATA if rows == 0 else PartitionStatus.EXTRACTED
    return {
        "endpoint_prefix": key.endpoint_prefix,
        "year": key.year,
        "month": key.month,
        "municipality_code": key.municipality_code,
        "status": status.value,
        "row_count": rows,
        "content_sha256": manifest["content_sha256"],
        "schema_version": SCHEMA_VERSION,
        "storage_uri": str(destination),
        "extracted_at": item.extracted_at,
        "columns": tuple(manifest.get("columns") or ()),
    }


def _batched(items: Iterable[Any], size: int) -> Iterator[list[Any]]:
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def adopt(
    source: Path | str,
    catalog: LegacyCatalog,
    *,
    storage: LocalStorage,
    repository: Any,
    run_id: str,
    endpoint_prefix: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    workers: int = 8,
    limit: int | None = None,
    chunk_size: int = 500,
) -> dict[str, Any]:
    """Copia, gera manifesto e registra estado para cada arquivo legado."""
    if workers < 1:
        raise ValueError("workers deve ser >= 1")
    if chunk_size < 1:
        raise ValueError("chunk_size deve ser >= 1")

    rejects: list[tuple[str, str]] = []
    items = scan(
        source,
        catalog,
        endpoint_prefix=endpoint_prefix,
        start_year=start_year,
        end_year=end_year,
        rejects=rejects,
    )
    if limit:
        items = (item for index, item in enumerate(items) if index < limit)

    started = time.monotonic()
    processed = written = preserved = no_data = failed = 0
    bytes_total = 0
    failures: list[tuple[str, str]] = []
    schemas: Counter[tuple[str, tuple[str, ...]]] = Counter()
    pending: list[dict[str, Any]] = []

    def flush() -> None:
        nonlocal pending, written, preserved
        if not pending:
            return
        result = repository.adopt_many(pending, run_id=run_id)
        written += result["written"]
        preserved += result["preserved"]
        pending = []

    with ThreadPoolExecutor(max_workers=workers) as pool:
        for batch in _batched(items, chunk_size):
            futures = [
                pool.submit(_adopt_one, item, storage=storage, run_id=run_id)
                for item in batch
            ]
            for item, future in zip(batch, futures):
                try:
                    record = future.result()
                except Exception as exc:  # noqa: BLE001 - segue nos demais arquivos
                    failed += 1
                    failures.append((item.source.name, f"{type(exc).__name__}: {exc}"))
                    continue
                processed += 1
                bytes_total += item.size_bytes
                if record["status"] == PartitionStatus.NO_DATA.value:
                    no_data += 1
                schemas[(record["endpoint_prefix"], record.pop("columns"))] += 1
                pending.append(record)
            flush()
            if processed == 0 or processed % 20000 < chunk_size:
                elapsed = time.monotonic() - started
                rate = processed / elapsed if elapsed else 0
                log.info(
                    "Migração: %s arquivos (%.0f/s), %s registrados, %s falhas",
                    processed,
                    rate,
                    written + preserved,
                    failed,
                )
    flush()

    elapsed = time.monotonic() - started
    drift = {
        prefix: [
            {"colunas": len(columns), "arquivos": count}
            for (name, columns), count in schemas.items()
            if name == prefix
        ]
        for prefix in sorted({name for name, _ in schemas})
    }
    return {
        "run_id": run_id,
        "processed": processed,
        "written": written,
        "preserved": preserved,
        "no_data": no_data,
        "failed": failed,
        "bytes": bytes_total,
        "seconds": round(elapsed, 1),
        "rejects_total": len(rejects),
        "rejects": rejects[:20],
        "failures": failures[:20],
        "schema_variants": {
            prefix: sorted(variants, key=lambda item: -item["arquivos"])
            for prefix, variants in drift.items()
        },
    }


def verify(
    catalog: LegacyCatalog,
    *,
    storage: LocalStorage,
    repository: Any,
    sample: int,
    endpoint_prefix: str | None = None,
    seed: int | None = None,
) -> dict[str, Any]:
    """Confere manifesto e SHA-256 de uma amostra do estado adotado."""
    if sample < 1:
        raise ValueError("sample deve ser >= 1")
    snapshots = [
        snapshot
        for snapshot in repository.list_all()
        if snapshot.status
        in (
            PartitionStatus.EXTRACTED,
            PartitionStatus.VALIDATED,
            PartitionStatus.PUBLISHED,
            PartitionStatus.NO_DATA,
        )
        and (not endpoint_prefix or snapshot.key.endpoint_prefix == endpoint_prefix)
    ]
    if not snapshots:
        return {"checked": 0, "mismatches": []}
    chosen = random.Random(seed).sample(snapshots, min(sample, len(snapshots)))
    mismatches: list[dict[str, Any]] = []
    for snapshot in chosen:
        key = snapshot.key
        key_name = storage_key(key)
        try:
            actual = storage.sha256(key_name)
        except Exception as exc:  # noqa: BLE001 - relatório, não exceção
            mismatches.append({"partition": key.filename, "erro": str(exc)})
            continue
        if actual != snapshot.content_sha256:
            mismatches.append(
                {
                    "partition": key.filename,
                    "esperado": snapshot.content_sha256,
                    "encontrado": actual,
                }
            )
    return {"checked": len(chosen), "mismatches": mismatches}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Adota os Parquets do extrator anterior no lakehouse."
    )
    parser.add_argument("--source", required=True, help="diretório legado (leitura)")
    parser.add_argument(
        "--storage-root",
        default=os.environ.get("TCE_PIPELINE_DATA_DIR"),
        help="raiz do lakehouse (padrão: TCE_PIPELINE_DATA_DIR)",
    )
    parser.add_argument("--endpoint", help="restringe a um endpoint do catálogo")
    parser.add_argument("--start-year", type=int)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--limit", type=int, help="teto de arquivos nesta execução")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true", help="só inventário")
    parser.add_argument(
        "--verify",
        type=int,
        metavar="AMOSTRA",
        help="confere SHA-256 de N partições já registradas e sai",
    )
    parser.add_argument("--json", action="store_true", help="saída JSON")
    parser.add_argument("--run-id", help="identificador da execução no estado")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    catalog = LegacyCatalog.load()
    if args.endpoint and args.endpoint not in catalog.endpoints:
        parser.error(f"endpoint fora do catálogo: {args.endpoint}")

    if args.dry_run:
        summary = inventory(
            args.source,
            catalog,
            endpoint_prefix=args.endpoint,
            start_year=args.start_year,
            end_year=args.end_year,
        )
        if args.json:
            print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
        else:
            print(format_inventory(summary))
        return 0

    if not args.storage_root:
        parser.error("--storage-root ou TCE_PIPELINE_DATA_DIR é obrigatório")
    storage = LocalStorage(args.storage_root)

    from pipeline_tce.state.repository import PartitionRepository

    repository = PartitionRepository.from_env()

    if args.verify:
        result = verify(
            catalog,
            storage=storage,
            repository=repository,
            sample=args.verify,
            endpoint_prefix=args.endpoint,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if not result["mismatches"] else 1

    repository.ensure_schema()
    run_id = args.run_id or (
        IMPORT_RUN_PREFIX + datetime.now(timezone.utc).isoformat(timespec="seconds")
    )
    summary = adopt(
        args.source,
        catalog,
        storage=storage,
        repository=repository,
        run_id=run_id,
        endpoint_prefix=args.endpoint,
        start_year=args.start_year,
        end_year=args.end_year,
        workers=args.workers,
        limit=args.limit,
    )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    else:
        print(
            f"run_id={summary['run_id']} arquivos={summary['processed']} "
            f"registrados={summary['written'] + summary['preserved']} "
            f"(novos={summary['written']}, preservados={summary['preserved']}) "
            f"no_data={summary['no_data']} falhas={summary['failed']} "
            f"{summary['bytes'] / 1e9:.2f} GB em {summary['seconds']}s"
        )
        for name, reason in summary["rejects"]:
            print(f"  rejeitado {name}: {reason}")
        for name, reason in summary["failures"]:
            print(f"  falha {name}: {reason}")
    return 0 if not summary["failed"] else 1


if __name__ == "__main__":  # pragma: no cover - ponto de entrada
    raise SystemExit(main())

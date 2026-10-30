"""Execução em lote de partições com teto, progresso e circuit breaker."""

from __future__ import annotations

import logging
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pipeline_tce.ingestion.catalog import Endpoint, Municipality
from pipeline_tce.ingestion.client import RequestPacer, RetryPolicy, extract_partition
from pipeline_tce.ingestion.planning import Partition, build_partition

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchPolicy:
    """Limites conservadores padrão; ajustar só com medição do destino."""

    max_workers: int = 3
    max_partitions: int = 500
    max_consecutive_failures: int = 15
    progress_every_seconds: float = 15.0
    progress_every_partitions: int = 100

    def __post_init__(self) -> None:
        if self.max_workers < 1:
            raise ValueError("max_workers deve ser positivo")
        if self.max_partitions < 1:
            raise ValueError("max_partitions deve ser positivo")
        if self.max_consecutive_failures < 1:
            raise ValueError("max_consecutive_failures deve ser positivo")
        if self.progress_every_seconds <= 0 or self.progress_every_partitions < 1:
            raise ValueError("Intervalos de progresso devem ser positivos")


def iter_partitions(
    endpoints: dict[str, Endpoint],
    municipalities: dict[str, Municipality],
    start_year: int,
    end_year: int,
    *,
    endpoint_prefixes: Iterable[str] | None = None,
    municipality_code: str | None = None,
) -> Iterator[Partition]:
    """Gera partições sob demanda, sem materializar a matriz inteira em memória."""
    if start_year < 2008 or end_year > 2100 or start_year > end_year:
        raise ValueError("Intervalo de anos inválido")
    if endpoint_prefixes:
        selected = []
        for prefix in endpoint_prefixes:
            if prefix not in endpoints:
                raise ValueError(f"Endpoint não cadastrado: {prefix}")
            selected.append(endpoints[prefix])
    else:
        selected = list(endpoints.values())

    if municipality_code:
        if municipality_code not in municipalities:
            raise ValueError(f"Município não cadastrado: {municipality_code}")
        targets = [municipalities[municipality_code]]
    else:
        targets = list(municipalities.values())
    if not selected or not targets:
        raise ValueError("Lote vazio: confirme endpoints e municípios")

    for endpoint in selected:
        for year in range(start_year, end_year + 1):
            months: Iterable[int | None]
            months = range(1, 13) if endpoint.frequency == "mensal" else (None,)
            for month in months:
                for municipality in targets:
                    yield build_partition(endpoint, year, month, municipality)


def _format_eta(seconds: float) -> str:
    total = max(0, int(seconds))
    return f"{total // 3600:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"


def run_batch(
    partitions: Iterable[Partition],
    output_dir: Path | str,
    *,
    policy: BatchPolicy | None = None,
    retry_policy: RetryPolicy | None = None,
    pacer: RequestPacer | None = None,
    extract: Callable[..., dict[str, Any]] = extract_partition,
) -> dict[str, Any]:
    """Processa o lote mantendo um progresso durável pelo próprio Parquet."""
    policy = policy or BatchPolicy()
    retry_policy = retry_policy or RetryPolicy()
    pacer = pacer or RequestPacer(retry_policy.min_interval_seconds)

    counters: Counter[str] = Counter()
    started = time.monotonic()
    last_progress = started
    submitted = processed = 0
    consecutive_failures = 0
    circuit_open = False
    plan_complete = False
    iterator = iter(partitions)

    with ThreadPoolExecutor(max_workers=policy.max_workers) as executor:
        in_flight: dict[Future[dict[str, Any]], Partition] = {}
        while True:
            while (
                not circuit_open
                and len(in_flight) < policy.max_workers
                and submitted < policy.max_partitions
            ):
                try:
                    partition = next(iterator)
                except StopIteration:
                    plan_complete = True
                    break
                in_flight[
                    executor.submit(
                        extract,
                        partition,
                        output_dir,
                        policy=retry_policy,
                        pacer=pacer,
                    )
                ] = partition
                submitted += 1

            if not in_flight:
                break

            done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
            for future in done:
                partition = in_flight.pop(future)
                processed += 1
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001 - agrega qualquer falha por partição
                    counters["failed"] += 1
                    consecutive_failures += 1
                    log.error(
                        "Falha na partição; endpoint=%s arquivo=%s erro=%s",
                        partition.endpoint.prefix,
                        partition.filename,
                        type(exc).__name__,
                    )
                    if (
                        not circuit_open
                        and consecutive_failures >= policy.max_consecutive_failures
                    ):
                        circuit_open = True
                        log.error(
                            "Circuit breaker aberto após %s falhas consecutivas; "
                            "nenhuma partição nova será iniciada. As concluídas permanecem "
                            "salvas e a próxima execução retoma o que falta.",
                            consecutive_failures,
                        )
                    continue

                status = str(result.get("status", "unknown"))
                counters[status] += 1
                consecutive_failures = 0
                log.info(
                    "Partição concluída; endpoint=%s arquivo=%s status=%s linhas=%s",
                    partition.endpoint.prefix,
                    partition.filename,
                    status,
                    result.get("rows", 0),
                )

            now = time.monotonic()
            if (
                processed % policy.progress_every_partitions == 0
                or now - last_progress >= policy.progress_every_seconds
            ):
                elapsed = max(now - started, 0.001)
                rate = processed / elapsed
                detail = f"taxa={rate:.1f} partições/s"
                if plan_complete:
                    detail += f" eta~{_format_eta((submitted - processed) / rate if rate else 0)}"
                log.info(
                    "PROGRESSO %s/%s %s | salvos=%s existentes=%s vazios=%s "
                    "sem_cache=%s falhas=%s | %s",
                    processed,
                    submitted,
                    f"({processed * 100 / submitted:.1f}%)" if submitted else "",
                    counters["downloaded"],
                    counters["already_present"],
                    counters["no_data_cached"],
                    counters["no_data_uncached"],
                    counters["failed"],
                    detail,
                )
                last_progress = now

    elapsed = max(time.monotonic() - started, 0.001)
    summary = {
        "submitted": submitted,
        "processed": processed,
        "plan_complete": plan_complete,
        "hit_partition_cap": not plan_complete and submitted >= policy.max_partitions,
        "circuit_breaker_open": circuit_open,
        "status_counts": dict(counters),
        "elapsed_seconds": round(elapsed, 1),
    }
    log.info(
        "RESUMO DO LOTE: submetidas=%s processadas=%s plano_completo=%s "
        "teto_atingido=%s circuit_breaker=%s baixados=%s existentes=%s "
        "vazios=%s sem_cache=%s falhas=%s duracao=%.1fs",
        submitted,
        processed,
        plan_complete,
        summary["hit_partition_cap"],
        circuit_open,
        counters["downloaded"],
        counters["already_present"],
        counters["no_data_cached"],
        counters["no_data_uncached"],
        counters["failed"],
        elapsed,
    )
    return summary

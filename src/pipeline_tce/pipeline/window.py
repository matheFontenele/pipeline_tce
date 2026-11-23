"""Atualização recorrente: reextrai sempre uma janela de meses recentes.

A API publica dados de forma incremental: o mês em curso muda todos os dias e o
mês recém-fechado ainda recebe revisão. Por isso a atualização periódica não
pode confiar no estado ``extracted``/``no_data`` — ela declara uma janela
explícita e reextrai essas partições de propósito (``refresh=True``), enquanto o
restante do histórico permanece imutável.
"""

from __future__ import annotations

import logging
import time
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from pipeline_tce.ingestion.catalog import Endpoint, Municipality
from pipeline_tce.ingestion.client import RequestPacer, RetryPolicy, extract_partition
from pipeline_tce.ingestion.planning import Partition, build_partition
from pipeline_tce.pipeline.partition import PendingDataError, run_partition

log = logging.getLogger(__name__)


def months_window(reference: date, months_back: int = 2) -> list[tuple[int, int]]:
    """Meses da janela, em ordem cronológica, terminando no mês de referência."""
    if months_back < 1:
        raise ValueError("months_back deve ser >= 1")
    year, month = reference.year, reference.month
    window: list[tuple[int, int]] = []
    for _ in range(months_back):
        window.append((year, month))
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    return sorted(window)


def window_partitions(
    endpoints: dict[str, Endpoint],
    municipalities: dict[str, Municipality],
    window: Sequence[tuple[int, int]],
    *,
    endpoint_prefixes: Iterable[str] | None = None,
) -> Iterator[Partition]:
    """Partições mensais da janela e, dos anuais, cada ano presente nela.

    Endpoint anual não tem recorte mensal: o ano corrente acumula durante o ano
    e o ano que virou é revisado em janeiro, então ambos os anos da janela são
    reextraídos (no máximo dois por município).
    """
    if not window:
        raise ValueError("Janela vazia")
    if endpoint_prefixes:
        selected = []
        for prefix in endpoint_prefixes:
            if prefix not in endpoints:
                raise ValueError(f"Endpoint não cadastrado: {prefix}")
            selected.append(endpoints[prefix])
    else:
        selected = list(endpoints.values())
    targets = list(municipalities.values())
    if not selected or not targets:
        raise ValueError("Janela vazia: confirme endpoints e municípios")

    window_years = sorted({year for year, _ in window})
    for endpoint in selected:
        if endpoint.frequency == "anual":
            for year in window_years:
                for municipality in targets:
                    yield build_partition(endpoint, year, None, municipality)
            continue
        for year, month in window:
            for municipality in targets:
                yield build_partition(endpoint, year, month, municipality)


@dataclass(frozen=True)
class WindowPolicy:
    """Limites da atualização diária; ajustar só com medição."""

    max_workers: int = 2
    max_consecutive_failures: int = 15
    max_partitions: int | None = None
    progress_every_partitions: int = 100
    progress_every_seconds: float = 60.0

    def __post_init__(self) -> None:
        if self.max_workers < 1:
            raise ValueError("max_workers deve ser positivo")
        if self.max_consecutive_failures < 1:
            raise ValueError("max_consecutive_failures deve ser positivo")
        if self.max_partitions is not None and self.max_partitions < 1:
            raise ValueError("max_partitions deve ser positivo quando informado")
        if self.progress_every_seconds <= 0 or self.progress_every_partitions < 1:
            raise ValueError("Intervalos de progresso devem ser positivos")


def _format_duration(seconds: float) -> str:
    total = int(max(seconds, 0))
    hours, rest = divmod(total, 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def run_window(
    partitions: Iterable[Partition],
    *,
    repository: Any,
    storage: Any,
    run_id: str,
    staging_dir: Path | str,
    policy: WindowPolicy | None = None,
    retry_policy: RetryPolicy | None = None,
    pacer: RequestPacer | None = None,
    refresh: bool = True,
    extract: Callable[..., dict[str, Any]] = extract_partition,
) -> dict[str, Any]:
    """Reextrai a janela com estado, storage e manifesto, retomando o que faltar.

    A falha de uma partição não interrompe a janela; após
    ``max_consecutive_failures`` seguidas nenhuma nova é iniciada, as concluídas
    permanecem salvas e a próxima execução retoma. O resultado nunca inclui
    arquivo parcial: a escrita é atômica e o manifesto vem por último.
    """
    if not run_id:
        raise ValueError("run_id é obrigatório")
    policy = policy or WindowPolicy()
    retry_policy = retry_policy or RetryPolicy()
    pacer = pacer or RequestPacer(retry_policy.min_interval_seconds)

    counters: Counter[str] = Counter()
    failures: list[dict[str, str]] = []
    started = time.monotonic()
    last_progress = started
    submitted = processed = 0
    consecutive_failures = 0
    circuit_open = False
    plan_complete = False
    iterator = iter(partitions)

    with ThreadPoolExecutor(max_workers=policy.max_workers) as executor:
        in_flight: dict[Future[Any], Partition] = {}
        while True:
            while (
                not circuit_open
                and len(in_flight) < policy.max_workers
                and (
                    policy.max_partitions is None
                    or submitted < policy.max_partitions
                )
            ):
                try:
                    partition = next(iterator)
                except StopIteration:
                    plan_complete = True
                    break
                in_flight[
                    executor.submit(
                        run_partition,
                        partition,
                        repository=repository,
                        storage=storage,
                        run_id=run_id,
                        staging_dir=Path(staging_dir),
                        extract=extract,
                        policy=retry_policy,
                        pacer=pacer,
                        refresh=refresh,
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
                except PendingDataError:
                    # Ausência no ano corrente: nada foi gravado e a próxima
                    # execução tenta de novo. Não é falha nem abre o circuito.
                    counters["pending_data"] += 1
                    consecutive_failures = 0
                    continue
                except Exception as exc:  # noqa: BLE001 - agrega falha por partição
                    counters["failed"] += 1
                    consecutive_failures += 1
                    failures.append(
                        {"partition": partition.filename, "error": type(exc).__name__}
                    )
                    log.error(
                        "Falha na atualização; endpoint=%s arquivo=%s erro=%s",
                        partition.endpoint.prefix,
                        partition.filename,
                        type(exc).__name__,
                    )
                    if consecutive_failures >= policy.max_consecutive_failures:
                        circuit_open = True
                        log.error(
                            "Circuit breaker aberto após %s falhas consecutivas; "
                            "nenhuma partição nova será iniciada.",
                            consecutive_failures,
                        )
                    continue
                counters[result.status] += 1
                consecutive_failures = 0

            now = time.monotonic()
            if (
                processed % policy.progress_every_partitions == 0
                or now - last_progress >= policy.progress_every_seconds
            ):
                last_progress = now
                elapsed = max(now - started, 0.001)
                rate = processed / elapsed
                restante = ""
                if plan_complete and rate:
                    restante = f" eta~{_format_duration((submitted - processed) / rate)}"
                log.info(
                    "ATUALIZAÇÃO %s/%s taxa=%.2f partições/s salvos=%s vazios=%s "
                    "sem_dados=%s pulados=%s falhas=%s%s",
                    processed,
                    submitted,
                    rate,
                    counters["extracted"],
                    counters["no_data"],
                    counters["pending_data"],
                    counters["skipped"],
                    counters["failed"],
                    restante,
                )

    elapsed = round(time.monotonic() - started, 1)
    summary = {
        "run_id": run_id,
        "processed": processed,
        "submitted": submitted,
        "extracted": counters["extracted"],
        "no_data": counters["no_data"],
        "pending_data": counters["pending_data"],
        "skipped": counters["skipped"],
        "failed": counters["failed"],
        "circuit_open": circuit_open,
        "plan_complete": plan_complete,
        "seconds": elapsed,
        "failures": failures[:20],
    }
    log.info(
        "Janela concluída; processos=%s extraídos=%s vazios=%s sem_dados=%s "
        "falhas=%s circuito_aberto=%s em %ss",
        processed,
        counters["extracted"],
        counters["no_data"],
        counters["pending_data"],
        counters["failed"],
        circuit_open,
        elapsed,
    )
    return summary

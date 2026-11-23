"""Relatório de progresso: o que foi baixado e o que falta.

Compara o universo planejado do catálogo com o estado de ingestão, sem chamar
a API e sem gravar nada. Uso típico dentro do container do Airflow::

    python -m pipeline_tce.reporting.status --start-year 2024 --end-year 2024 \\
        --municipality 057

``Feitas`` soma artefatos íntegros (``extracted``, ``validated``,
``published``) e ausência histórica (``no_data``). ``Faltam`` é
``planejadas - feitas``, incluindo registros ainda ``pending``/``running``,
falhas e partições que nunca entraram no estado.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Iterable, Mapping, Sequence

from pipeline_tce.ingestion.catalog import Endpoint, Municipality
from pipeline_tce.ingestion.planning import build_partition
from pipeline_tce.state.models import PartitionKey, PartitionSnapshot

# Maior prefixo do catálogo tem 60 caracteres; nomes mais longos são cortados
# para não desalinhar as colunas numéricas.
ENDPOINT_WIDTH = 60


@dataclass
class EndpointTotals:
    """Contadores de um endpoint no intervalo consultado."""

    planned: int = 0
    extracted: int = 0
    validated: int = 0
    published: int = 0
    no_data: int = 0
    failed: int = 0
    running: int = 0
    pending: int = 0
    unregistered: int = 0

    @property
    def done(self) -> int:
        return self.extracted + self.validated + self.published + self.no_data

    @property
    def remaining(self) -> int:
        return self.planned - self.done

    def add(self, other: "EndpointTotals") -> None:
        for name in self.__dataclass_fields__:
            setattr(self, name, getattr(self, name) + getattr(other, name))


@dataclass
class StatusReport:
    """Resultado completo, com totais por endpoint e agregado."""

    totals: dict[str, EndpointTotals] = field(default_factory=dict)
    overall: EndpointTotals = field(default_factory=EndpointTotals)
    out_of_scope: int = 0
    start_year: int = 0
    end_year: int = 0
    endpoint_filter: str | None = None
    municipality_filter: str | None = None

    def as_dict(self) -> dict:
        return {
            "start_year": self.start_year,
            "end_year": self.end_year,
            "endpoint_filter": self.endpoint_filter,
            "municipality_filter": self.municipality_filter,
            "out_of_scope": self.out_of_scope,
            "overall": asdict(self.overall) | {
                "done": self.overall.done,
                "remaining": self.overall.remaining,
            },
            "endpoints": {
                prefix: asdict(totals)
                | {"done": totals.done, "remaining": totals.remaining}
                for prefix, totals in self.totals.items()
            },
        }


def _state_tuple(key: PartitionKey) -> tuple[str, int, int | None, str]:
    return (key.endpoint_prefix, key.year, key.month, key.municipality_code)


def _iter_keys(
    endpoints: Mapping[str, Endpoint],
    municipalities: Mapping[str, Municipality],
    start_year: int,
    end_year: int,
    *,
    endpoint_prefix: str | None = None,
    municipality_code: str | None = None,
) -> Iterable[PartitionKey]:
    for endpoint in endpoints.values():
        if endpoint_prefix and endpoint.prefix != endpoint_prefix:
            continue
        months: Iterable[int | None]
        months = range(1, 13) if endpoint.frequency == "mensal" else (None,)
        for year in range(start_year, end_year + 1):
            for month in months:
                for municipality in municipalities.values():
                    if municipality_code and municipality.code != municipality_code:
                        continue
                    partition = build_partition(endpoint, year, month, municipality)
                    yield PartitionKey.from_partition(partition)


def build_report(
    *,
    endpoints: Mapping[str, Endpoint],
    municipalities: Mapping[str, Municipality],
    snapshots: Sequence[PartitionSnapshot],
    start_year: int,
    end_year: int,
    endpoint_prefix: str | None = None,
    municipality_code: str | None = None,
) -> StatusReport:
    """Cruza universo planejado e estado sem tocar em API, disco ou banco."""
    state = {_state_tuple(snapshot.key): snapshot.status for snapshot in snapshots}
    report = StatusReport(
        start_year=start_year,
        end_year=end_year,
        endpoint_filter=endpoint_prefix,
        municipality_filter=municipality_code,
    )

    for key in _iter_keys(
        endpoints,
        municipalities,
        start_year,
        end_year,
        endpoint_prefix=endpoint_prefix,
        municipality_code=municipality_code,
    ):
        totals = report.totals.setdefault(key.endpoint_prefix, EndpointTotals())
        totals.planned += 1
        status = state.pop(_state_tuple(key), None)
        if status is None:
            totals.unregistered += 1
        else:
            setattr(totals, status.value, getattr(totals, status.value) + 1)

    report.out_of_scope = len(state)
    for prefix in sorted(report.totals):
        report.overall.add(report.totals[prefix])
    report.totals = dict(sorted(report.totals.items()))
    return report


def format_report(report: StatusReport) -> str:
    """Tabela de texto simples, legível no terminal e nos logs."""
    header = (
        f"{'endpoint':<{ENDPOINT_WIDTH}}{'planejadas':>11}{'feitas':>8}{'faltam':>8}"
        f"{'extraidas':>10}{'no_data':>9}{'falhas':>8}{'rodando':>9}"
        f"{'pendentes':>10}{'sem_registro':>13}"
    )
    lines = [header, "-" * len(header)]
    for prefix, totals in report.totals.items():
        lines.append(
            f"{prefix[:ENDPOINT_WIDTH]:<{ENDPOINT_WIDTH}}{totals.planned:>11}{totals.done:>8}"
            f"{totals.remaining:>8}{totals.extracted:>10}{totals.no_data:>9}"
            f"{totals.failed:>8}{totals.running:>9}{totals.pending:>10}"
            f"{totals.unregistered:>13}"
        )
    overall = report.overall
    lines.append("-" * len(header))
    lines.append(
        f"{'TOTAL':<{ENDPOINT_WIDTH}}{overall.planned:>11}{overall.done:>8}"
        f"{overall.remaining:>8}{overall.extracted:>10}{overall.no_data:>9}"
        f"{overall.failed:>8}{overall.running:>9}{overall.pending:>10}"
        f"{overall.unregistered:>13}"
    )
    scope = f"{report.start_year}–{report.end_year}"
    if report.endpoint_filter:
        scope += f" endpoint={report.endpoint_filter}"
    if report.municipality_filter:
        scope += f" município={report.municipality_filter}"
    lines.append("")
    lines.append(f"Escopo: {scope}")
    if report.out_of_scope:
        lines.append(
            f"Aviso: {report.out_of_scope} registro(s) de estado fora do escopo "
            "não entram nos totais"
        )
    return "\n".join(lines)


def format_snapshots(snapshots: Sequence[PartitionSnapshot]) -> str:
    """Lista os registros de estado do escopo, um por linha."""
    header = (
        f"{'endpoint':<{ENDPOINT_WIDTH}}{'ano':>6}{'mes':>5}{'municipio':>11}"
        f"{'estado':>12}{'tentativas':>12}{'linhas':>9}  {'atualizado':<19} run"
    )
    lines = [header, "-" * len(header)]
    for snapshot in snapshots:
        key = snapshot.key
        lines.append(
            f"{key.endpoint_prefix[:ENDPOINT_WIDTH]:<{ENDPOINT_WIDTH}}{key.year:>6}"
            f"{(key.month if key.month is not None else '-'):>5}"
            f"{key.municipality_code:>11}{snapshot.status.value:>12}"
            f"{snapshot.attempts:>12}"
            f"{(snapshot.row_count if snapshot.row_count is not None else '-'):>9}"
            f"  {snapshot.updated_at:%Y-%m-%d %H:%M:%S} "
            f"{snapshot.last_run_id or '-'}"
        )
    if len(lines) == 2:
        lines.append("(nenhum registro de estado no escopo)")
    return "\n".join(lines)


def _in_scope(
    snapshot: PartitionSnapshot,
    start_year: int,
    end_year: int,
    endpoint_prefix: str | None,
    municipality_code: str | None,
) -> bool:
    key = snapshot.key
    if not start_year <= key.year <= end_year:
        return False
    if endpoint_prefix and key.endpoint_prefix != endpoint_prefix:
        return False
    if municipality_code and key.municipality_code != municipality_code:
        return False
    return True


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Relatório de progresso da extração por estado (sem API)."
    )
    parser.add_argument("--start-year", type=int, default=2008)
    parser.add_argument("--end-year", type=int, default=date.today().year)
    parser.add_argument("--endpoint", help="restringe a um endpoint do catálogo")
    parser.add_argument("--municipality", help="código de município com 3 dígitos")
    parser.add_argument("--list", action="store_true", help="lista os registros de estado")
    parser.add_argument("--json", action="store_true", help="saída JSON")
    args = parser.parse_args(argv)

    from pipeline_tce.ingestion.catalog import load_endpoints, load_municipalities
    from pipeline_tce.state.repository import PartitionRepository

    if args.start_year > args.end_year:
        parser.error("--start-year não pode ser maior que --end-year")

    endpoints = load_endpoints()
    municipalities = load_municipalities()
    endpoint_prefix = args.endpoint
    municipality_code = args.municipality.zfill(3) if args.municipality else None
    if endpoint_prefix and endpoint_prefix not in endpoints:
        parser.error(f"endpoint fora do catálogo: {endpoint_prefix}")
    if municipality_code and municipality_code not in municipalities:
        parser.error(f"município fora do catálogo: {municipality_code}")

    snapshots = PartitionRepository.from_env().list_all()
    scoped = [
        snapshot
        for snapshot in snapshots
        if _in_scope(
            snapshot,
            args.start_year,
            args.end_year,
            endpoint_prefix,
            municipality_code,
        )
    ]
    report = build_report(
        endpoints=endpoints,
        municipalities=municipalities,
        snapshots=snapshots,
        start_year=args.start_year,
        end_year=args.end_year,
        endpoint_prefix=endpoint_prefix,
        municipality_code=municipality_code,
    )

    if args.json:
        payload = report.as_dict()
        if args.list:
            payload["partitions"] = [
                {
                    "endpoint_prefix": s.key.endpoint_prefix,
                    "year": s.key.year,
                    "month": s.key.month,
                    "municipality_code": s.key.municipality_code,
                    "status": s.status.value,
                    "attempts": s.attempts,
                    "row_count": s.row_count,
                    "last_run_id": s.last_run_id,
                    "updated_at": s.updated_at.isoformat(),
                }
                for s in scoped
            ]
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print(format_report(report))
    if args.list:
        print()
        print(format_snapshots(scoped))
    return 0


if __name__ == "__main__":  # pragma: no cover - ponto de entrada
    raise SystemExit(main())

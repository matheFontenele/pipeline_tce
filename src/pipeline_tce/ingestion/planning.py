"""Planeja uma partição sem gerar milhares de tasks no scheduler."""

from __future__ import annotations

import calendar
from dataclasses import dataclass

from pipeline_tce.ingestion.catalog import Endpoint, Municipality


@dataclass(frozen=True)
class Partition:
    endpoint: Endpoint
    year: int
    month: int | None
    municipality: Municipality
    params: dict[str, int | str]

    @property
    def filename(self) -> str:
        if self.month is None:
            suffix = f"{self.year}_{self.municipality.code}"
        else:
            suffix = f"{self.year}_{self.month:02d}_{self.municipality.code}"
        return f"{self.endpoint.prefix}_{suffix}.parquet"


def build_partition(
    endpoint: Endpoint,
    year: int,
    month: int | None,
    municipality: Municipality,
) -> Partition:
    if year < 2008 or year > 9999:
        raise ValueError("O ano deve estar entre 2008 e 9999")
    if endpoint.frequency == "mensal" and (month is None or not 1 <= month <= 12):
        raise ValueError("Endpoint mensal exige mês entre 1 e 12")
    if endpoint.frequency == "anual" and month is not None:
        raise ValueError("Endpoint anual não aceita mês")

    params: dict[str, int | str] = {
        "$format": "json",
        "$count": 1000,
        "$start_index": 0,
    }
    for name in endpoint.required_parameters:
        if name == "exercicio_orcamento":
            params[name] = int(f"{year}00")
        elif name == "codigo_municipio":
            params[name] = municipality.code
        elif name == "data_referencia_doc":
            if month is None:
                raise ValueError("data_referencia_doc requer partição mensal")
            params[name] = int(f"{year}{month:02d}")
        elif name == "data_inicio":
            start_month = month or 1
            params[name] = f"{year:04d}{start_month:02d}01"
        elif name == "data_fim":
            end_month = month or 12
            last_day = calendar.monthrange(year, end_month)[1]
            params[name] = f"{year:04d}{end_month:02d}{last_day:02d}"
        else:  # O catálogo deve rejeitar isso antes do planejamento.
            raise ValueError(f"Parâmetro não implementado: {name}")

    return Partition(endpoint, year, month, municipality, params)

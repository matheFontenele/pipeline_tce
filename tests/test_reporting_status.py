from datetime import datetime, timezone

from pipeline_tce.ingestion.catalog import Endpoint, Municipality
from pipeline_tce.reporting.status import (
    EndpointTotals,
    build_report,
    format_report,
    format_snapshots,
)
from pipeline_tce.state.models import PartitionKey, PartitionSnapshot, PartitionStatus

MENSAIS = Endpoint("Mensal", "https://exemplo/mensal", "mensal", "mensal", ())
ANUAL = Endpoint("Anual", "https://exemplo/anual", "anual", "anual", ())
MUNICIPIOS = {
    "001": Municipality("001", "UM"),
    "057": Municipality("057", "FORTALEZA"),
}
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _snapshot(endpoint, year, month, municipality, status, rows=None):
    return PartitionSnapshot(
        key=PartitionKey(endpoint, year, month, municipality),
        status=status,
        attempts=1,
        row_count=rows,
        content_sha256=None,
        schema_version=None,
        storage_uri=None,
        last_run_id="run-1",
        last_error=None,
        created_at=NOW,
        updated_at=NOW,
        extracted_at=None,
        published_at=None,
    )


def _report(snapshots, **kwargs):
    return build_report(
        endpoints={"mensal": MENSAIS, "anual": ANUAL},
        municipalities=MUNICIPIOS,
        snapshots=snapshots,
        start_year=2024,
        end_year=2024,
        **kwargs,
    )


def test_conta_planejado_e_cruza_estado():
    report = _report(
        [
            _snapshot("mensal", 2024, 1, "057", PartitionStatus.EXTRACTED, rows=10),
            _snapshot("mensal", 2024, 2, "057", PartitionStatus.NO_DATA),
            _snapshot("mensal", 2024, 1, "001", PartitionStatus.FAILED),
            _snapshot("anual", 2024, None, "057", PartitionStatus.PUBLISHED, rows=3),
        ]
    )

    mensal = report.totals["mensal"]
    assert mensal.planned == 24  # 12 meses x 2 municípios
    assert mensal.extracted == 1
    assert mensal.no_data == 1
    assert mensal.failed == 1
    assert mensal.unregistered == 21
    assert mensal.done == 2
    assert mensal.remaining == 22

    anual = report.totals["anual"]
    assert anual.planned == 2
    assert anual.published == 1
    assert anual.unregistered == 1

    assert report.overall.planned == 26
    assert report.overall.done == 3
    assert report.overall.remaining == 23
    assert report.out_of_scope == 0


def test_registros_fora_do_escopo_nao_entram_nos_totais():
    report = _report(
        [
            _snapshot("mensal", 2023, 1, "057", PartitionStatus.EXTRACTED),
            _snapshot("mensal", 2024, 1, "057", PartitionStatus.EXTRACTED),
        ]
    )

    assert report.out_of_scope == 1
    assert report.overall.done == 1
    assert report.overall.planned == 26


def test_filtros_por_endpoint_e_municipio():
    report = _report(
        [
            _snapshot("mensal", 2024, 1, "057", PartitionStatus.EXTRACTED),
            _snapshot("mensal", 2024, 1, "001", PartitionStatus.EXTRACTED),
        ],
        endpoint_prefix="mensal",
        municipality_code="057",
    )

    assert set(report.totals) == {"mensal"}
    mensal = report.totals["mensal"]
    assert mensal.planned == 12
    assert mensal.extracted == 1
    assert mensal.remaining == 11
    assert report.out_of_scope == 1  # registro do município 001


def test_formatacao_inclui_total_e_aviso_de_escopo():
    report = _report(
        [_snapshot("mensal", 2023, 1, "057", PartitionStatus.EXTRACTED)]
    )
    texto = format_report(report)

    assert "TOTAL" in texto
    assert "mensal" in texto
    assert "fora do escopo" in texto


def test_listagem_de_snapshots():
    snapshots = [
        _snapshot("mensal", 2024, 1, "057", PartitionStatus.EXTRACTED, rows=10),
        _snapshot("anual", 2024, None, "057", PartitionStatus.NO_DATA),
    ]

    texto = format_snapshots(snapshots)

    assert "extracted" in texto
    assert "no_data" in texto
    assert "(-)" in texto  # mês nulo para anual


def test_endpoint_totals_soma():
    total = EndpointTotals(planned=5, extracted=2)
    total.add(EndpointTotals(planned=3, extracted=1, failed=1))

    assert total.planned == 8
    assert total.extracted == 3
    assert total.failed == 1
    assert total.done == 3
    assert total.remaining == 5

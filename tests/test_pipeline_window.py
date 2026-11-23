import json
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from pipeline_tce.ingestion.catalog import Endpoint, Municipality
from pipeline_tce.ingestion.planning import build_partition
from pipeline_tce.pipeline.partition import storage_key
from pipeline_tce.pipeline.window import (
    WindowPolicy,
    months_window,
    run_window,
    window_partitions,
)
from pipeline_tce.state.models import PartitionKey, PartitionSnapshot, PartitionStatus
from pipeline_tce.storage import LocalStorage, manifest_key

MENSAL = Endpoint(
    "Notas de Empenho",
    "https://api-dados-abertos.tce.ce.gov.br/sim/notas_empenhos",
    "notas_empenho",
    "mensal",
    ("exercicio_orcamento",),
)
ANUAL = Endpoint(
    "Itens de Notas Fiscais",
    "https://api-dados-abertos.tce.ce.gov.br/sim/itens_notas_fiscais",
    "itens_notas_fiscais",
    "anual",
    ("exercicio_orcamento",),
)
ENDPOINTS = {MENSAL.prefix: MENSAL, ANUAL.prefix: ANUAL}
MUNICIPALITIES = {
    "057": Municipality("057", "FORTALEZA"),
    "001": Municipality("001", "ACARAPE"),
}
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_janela_termina_no_mes_de_referencia():
    assert months_window(date(2026, 11, 23), 2) == [(2026, 10), (2026, 11)]
    assert months_window(date(2026, 11, 23), 1) == [(2026, 11)]
    assert months_window(date(2026, 11, 23), 4) == [
        (2026, 8),
        (2026, 9),
        (2026, 10),
        (2026, 11),
    ]


def test_janela_cruza_virada_de_ano():
    assert months_window(date(2027, 1, 5), 3) == [
        (2026, 11),
        (2026, 12),
        (2027, 1),
    ]


@pytest.mark.parametrize("quantidade", [0, -1])
def test_janela_rejeita_quantidade_invalida(quantidade):
    with pytest.raises(ValueError, match="months_back"):
        months_window(date(2026, 11, 23), quantidade)


def test_janela_gera_mensais_e_anuais_sem_duplicar():
    partitions = list(
        window_partitions(
            ENDPOINTS,
            MUNICIPALITIES,
            [(2025, 12), (2026, 1)],
        )
    )

    mensais = [p for p in partitions if p.endpoint.prefix == "notas_empenho"]
    anuais = [p for p in partitions if p.endpoint.prefix == "itens_notas_fiscais"]

    assert len(mensais) == 2 * 2 * 2  # meses × municípios
    assert {(p.year, p.month) for p in mensais} == {(2025, 12), (2026, 1)}
    # Um registro anual por município e por ano da janela, nunca por mês.
    assert len(anuais) == 2 * 2
    assert {(p.year, p.month) for p in anuais} == {(2025, None), (2026, None)}


def test_janela_rejeita_endpoint_ou_janela_invalida():
    with pytest.raises(ValueError, match="Janela vazia"):
        list(window_partitions(ENDPOINTS, MUNICIPALITIES, []))
    with pytest.raises(ValueError, match="não cadastrado"):
        list(
            window_partitions(
                ENDPOINTS, MUNICIPALITIES, [(2026, 1)], endpoint_prefixes=["inexistente"]
            )
        )


def _snapshot(status, **overrides):
    values = {
        "key": PartitionKey("notas_empenho", 2026, 11, "057"),
        "status": status,
        "attempts": 0,
        "row_count": None,
        "content_sha256": None,
        "schema_version": None,
        "storage_uri": None,
        "last_run_id": None,
        "last_error": None,
        "created_at": NOW,
        "updated_at": NOW,
        "extracted_at": None,
        "published_at": None,
    }
    values.update(overrides)
    return PartitionSnapshot(**values)


def _extract(rows=1, status=None):
    def extract(partition, output_dir, **kwargs):
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        if status == "no_data_uncached":
            return {"status": "no_data_uncached", "file": partition.filename, "rows": 0}
        frame = pd.DataFrame({"valor": list(range(rows))})
        frame.to_parquet(output_dir / partition.filename, index=False)
        return {"status": status or "downloaded", "file": partition.filename, "rows": rows}

    return extract


def _failing(partition, output_dir, **kwargs):
    raise RuntimeError("falha simulada")


def _repository(status=PartitionStatus.PENDING):
    fake = Mock()
    fake.get.return_value = _snapshot(status)
    return fake


def _window_partitions(total):
    partitions = []
    for year in range(2020, 2031):
        for month in (1, 2):
            for code in MUNICIPALITIES:
                partitions.append(
                    build_partition(MENSAL, year, month, MUNICIPALITIES[code])
                )
    assert len(set(partitions)) == len(partitions)
    return partitions[:total]


def test_janela_grava_estado_storage_e_manifesto(tmp_path):
    repository = _repository()
    storage = LocalStorage(tmp_path / "data")

    summary = run_window(
        _window_partitions(2),
        repository=repository,
        storage=storage,
        run_id="run-1",
        staging_dir=tmp_path / "staging",
        policy=WindowPolicy(max_workers=1),
        extract=_extract(3),
    )

    assert summary["processed"] == 2
    assert summary["extracted"] == 2
    assert summary["failed"] == 0
    assert summary["circuit_open"] is False
    assert repository.mark_extracted.call_count == 2
    assert repository.request_reprocess.call_count == 0

    chave = storage_key(repository.mark_extracted.call_args.args[0])
    manifesto = json.loads(storage.read_bytes(manifest_key(chave)))
    assert manifesto["row_count"] == 3


def test_janela_com_refresh_reextrai_particao_com_artefato(tmp_path):
    repository = _repository(PartitionStatus.EXTRACTED)

    summary = run_window(
        _window_partitions(1),
        repository=repository,
        storage=LocalStorage(tmp_path / "data"),
        run_id="run-1",
        staging_dir=tmp_path / "staging",
        policy=WindowPolicy(max_workers=1),
        extract=_extract(2),
        refresh=True,
    )

    assert summary["extracted"] == 1
    assert summary["skipped"] == 0
    repository.request_reprocess.assert_called_once()
    repository.mark_running.assert_called_once()


def test_janela_sem_refresh_pula_particao_com_artefato(tmp_path):
    repository = _repository(PartitionStatus.EXTRACTED)

    summary = run_window(
        _window_partitions(1),
        repository=repository,
        storage=LocalStorage(tmp_path / "data"),
        run_id="run-1",
        staging_dir=tmp_path / "staging",
        policy=WindowPolicy(max_workers=1),
        extract=_extract(2),
        refresh=False,
    )

    assert summary["skipped"] == 1
    assert summary["extracted"] == 0
    repository.mark_running.assert_not_called()


def test_ausencia_no_ano_corrente_nao_abre_circuito(tmp_path):
    repository = _repository()

    summary = run_window(
        _window_partitions(6),
        repository=repository,
        storage=LocalStorage(tmp_path / "data"),
        run_id="run-1",
        staging_dir=tmp_path / "staging",
        policy=WindowPolicy(max_workers=1, max_consecutive_failures=2),
        extract=_extract(status="no_data_uncached"),
    )

    assert summary["pending_data"] == 6
    assert summary["failed"] == 0
    assert summary["circuit_open"] is False
    assert summary["plan_complete"] is True
    assert repository.mark_failed.call_count == 6


def test_falhas_consecutivas_abrem_circuito(tmp_path):
    repository = _repository()

    summary = run_window(
        _window_partitions(20),
        repository=repository,
        storage=LocalStorage(tmp_path / "data"),
        run_id="run-1",
        staging_dir=tmp_path / "staging",
        policy=WindowPolicy(max_workers=1, max_consecutive_failures=3),
        extract=_failing,
    )

    assert summary["circuit_open"] is True
    assert summary["plan_complete"] is False
    assert summary["failed"] == 3
    assert summary["submitted"] == 3

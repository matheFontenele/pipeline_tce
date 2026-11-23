import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from pipeline_tce.ingestion.catalog import Endpoint, Municipality
from pipeline_tce.ingestion.planning import build_partition
from pipeline_tce.pipeline.partition import (
    PipelineError,
    run_partition,
    storage_key,
    verify_partition,
)
from pipeline_tce.state.models import (
    PartitionKey,
    PartitionSnapshot,
    PartitionStatus,
)
from pipeline_tce.storage import LocalStorage, manifest_key

ENDPOINT = Endpoint(
    "Notas de Empenho",
    "https://api-dados-abertos.tce.ce.gov.br/sim/notas_empenhos",
    "notas_empenho",
    "mensal",
    ("exercicio_orcamento", "codigo_municipio", "data_referencia_doc"),
)
MUNICIPALITY = Municipality("057", "FORTALEZA")
PARTITION = build_partition(ENDPOINT, 2024, 1, MUNICIPALITY)
KEY = PartitionKey.from_partition(PARTITION)


def _snapshot(status, **overrides):
    now = datetime(2026, 1, 1, tzinfo=timezone.utc)
    values = {
        "key": KEY,
        "status": status,
        "attempts": 0,
        "row_count": None,
        "content_sha256": None,
        "schema_version": None,
        "storage_uri": None,
        "last_run_id": None,
        "last_error": None,
        "created_at": now,
        "updated_at": now,
        "extracted_at": None,
        "published_at": None,
    }
    values.update(overrides)
    return PartitionSnapshot(**values)


def _extract(rows, status=None):
    def extract(partition, output_dir, **kwargs):
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        frame = pd.DataFrame({"valor": list(range(rows))}) if rows else pd.DataFrame()
        frame.to_parquet(output_dir / partition.filename, index=False)
        return {
            "status": status or ("downloaded" if rows else "no_data_cached"),
            "file": partition.filename,
            "rows": rows,
        }

    return extract


@pytest.fixture
def repository():
    fake = Mock()
    fake.get.return_value = _snapshot(PartitionStatus.PENDING)
    return fake


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(tmp_path / "data")


def _run(repository, storage, tmp_path, extract):
    return run_partition(
        PARTITION,
        repository=repository,
        storage=storage,
        run_id="run-1",
        staging_dir=tmp_path / "staging",
        extract=extract,
    )


def test_grava_parquet_manifesto_e_marca_estado(repository, storage, tmp_path):
    result = _run(repository, storage, tmp_path, _extract(3))

    assert result.status == "extracted"
    assert result.rows == 3
    repository.ensure.assert_called_once()
    repository.mark_running.assert_called_once_with(KEY, "run-1")

    kwargs = repository.mark_extracted.call_args.kwargs
    assert kwargs["row_count"] == 3
    assert kwargs["schema_version"] == "1"
    assert len(kwargs["content_sha256"]) == 64

    assert storage.exists(storage_key(KEY))
    manifest = json.loads(storage.read_bytes(manifest_key(storage_key(KEY))))
    assert manifest["row_count"] == 3
    assert manifest["content_sha256"] == kwargs["content_sha256"]
    assert list(Path(tmp_path / "staging").iterdir()) == []


def test_particao_publicada_e_pulada_sem_chamar_api(repository, storage, tmp_path):
    repository.get.return_value = _snapshot(
        PartitionStatus.PUBLISHED, row_count=9, storage_uri="/publicado"
    )
    extract = Mock()

    result = _run(repository, storage, tmp_path, extract)

    assert result.status == "skipped"
    assert result.rows == 9
    extract.assert_not_called()
    repository.mark_running.assert_not_called()


@pytest.mark.parametrize(
    "status", [PartitionStatus.EXTRACTED, PartitionStatus.VALIDATED]
)
def test_particao_com_artefato_e_pulada_sem_reextrair(
    repository, storage, tmp_path, status
):
    repository.get.return_value = _snapshot(
        status, row_count=4, content_sha256="b" * 64, storage_uri="/bronze"
    )
    extract = Mock()

    result = _run(repository, storage, tmp_path, extract)

    assert result.status == "skipped"
    assert result.rows == 4
    extract.assert_not_called()
    repository.mark_running.assert_not_called()
    repository.mark_failed.assert_not_called()


def test_estado_pending_apos_pedido_explicito_reextrai_particao(
    repository, storage, tmp_path
):
    # request_reprocess levou a partição de extracted para pending.
    repository.get.return_value = _snapshot(
        PartitionStatus.PENDING, row_count=4, content_sha256="b" * 64
    )

    result = _run(repository, storage, tmp_path, _extract(3))

    assert result.status == "extracted"
    assert result.rows == 3
    repository.mark_running.assert_called_once_with(KEY, "run-1")
    repository.mark_failed.assert_not_called()


def test_ausencia_historica_vira_no_data_com_marcador(repository, storage, tmp_path):
    result = _run(repository, storage, tmp_path, _extract(0))

    assert result.status == "no_data"
    assert result.rows == 0
    repository.mark_no_data.assert_called_once_with(KEY, run_id="run-1")
    repository.mark_extracted.assert_not_called()
    manifest = json.loads(storage.read_bytes(manifest_key(storage_key(KEY))))
    assert manifest["row_count"] == 0


def test_ausencia_no_ano_corrente_falha_sem_gravar(repository, storage, tmp_path):
    # Simula o contrato real: no ano corrente não há arquivo de marcador.
    def no_file_extract(partition, output_dir, **kwargs):
        return {"status": "no_data_uncached", "file": partition.filename, "rows": 0}

    with pytest.raises(PipelineError, match="ano corrente"):
        _run(repository, storage, tmp_path, no_file_extract)

    repository.mark_failed.assert_called_once()
    assert storage.list_keys() == []
    assert list(Path(tmp_path / "staging").iterdir()) == []


def test_falha_de_extracao_marca_failed_e_nao_grava(repository, storage, tmp_path):
    def failing(partition, output_dir, **kwargs):
        raise RuntimeError("falha simulada")

    with pytest.raises(RuntimeError, match="falha simulada"):
        _run(repository, storage, tmp_path, failing)

    repository.mark_failed.assert_called_once()
    assert repository.mark_failed.call_args.kwargs["error"] == "RuntimeError: falha simulada"
    assert storage.list_keys() == []


def test_conferencia_detecta_arquivo_adulterado(repository, storage, tmp_path):
    _run(repository, storage, tmp_path, _extract(2))
    sha = json.loads(storage.read_bytes(manifest_key(storage_key(KEY))))[
        "content_sha256"
    ]
    repository.get.return_value = _snapshot(
        PartitionStatus.EXTRACTED, row_count=2, content_sha256=sha
    )

    verification = verify_partition(KEY, repository=repository, storage=storage)
    assert verification == {"status": "extracted", "rows": 2, "sha256": sha}

    path = Path(storage.uri(storage_key(KEY)))
    path.write_bytes(b"adulterado")
    with pytest.raises(PipelineError, match="diverge do manifesto"):
        verify_partition(KEY, repository=repository, storage=storage)


def test_conferencia_de_ausencia_historica(repository, storage):
    repository.get.return_value = _snapshot(PartitionStatus.NO_DATA, row_count=0)

    assert verify_partition(KEY, repository=repository, storage=storage) == {
        "status": "no_data",
        "rows": 0,
        "sha256": None,
    }

import json
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pipeline_tce.ingestion.catalog import Endpoint, Municipality
from pipeline_tce.migration.legacy import (
    LegacyCatalog,
    LegacyNameError,
    adopt,
    format_inventory,
    inventory,
    parse_filename,
    scan,
    verify,
)
from pipeline_tce.state.models import PartitionKey, PartitionSnapshot, PartitionStatus
from pipeline_tce.storage.local import LocalStorage

MENSAL = Endpoint("Mensal", "https://exemplo/mensal", "mensal", "mensal", ())
ANUAL = Endpoint("Anual", "https://exemplo/anual", "anual", "anual", ())
MUNICIPIOS = {
    "001": Municipality("001", "UM"),
    "057": Municipality("057", "FORTALEZA"),
}
CATALOG = LegacyCatalog(
    endpoints={"mensal": MENSAL, "anual": ANUAL},
    municipalities=MUNICIPIOS,
    prefixes=("mensal", "anual"),
)
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class FakeRepository:
    def __init__(self, snapshots=None):
        self.calls = []
        self.snapshots = snapshots or []

    def adopt_many(self, records, *, run_id):
        self.calls.append((run_id, [dict(record) for record in records]))
        return {"written": len(records), "preserved": 0}

    def list_all(self):
        return self.snapshots

    @property
    def records(self):
        return [record for _, batch in self.calls for record in batch]


def _parquet(path, rows=2):
    table = pa.table({"valor": pa.array(range(rows), type=pa.int64())})
    pq.write_table(table, path)
    return path


def _snapshot(key, status, sha=None):
    return PartitionSnapshot(
        key=key,
        status=status,
        attempts=0,
        row_count=2,
        content_sha256=sha,
        schema_version="1",
        storage_uri=None,
        last_run_id="import__teste",
        last_error=None,
        created_at=NOW,
        updated_at=NOW,
        extracted_at=NOW,
        published_at=None,
    )


def test_nome_mensal_e_anual():
    mensal = parse_filename("mensal_2024_01_057.parquet", CATALOG)
    assert mensal == PartitionKey("mensal", 2024, 1, "057")
    assert mensal.filename == "mensal_2024_01_057.parquet"

    anual = parse_filename("anual_2024_001.parquet", CATALOG)
    assert anual == PartitionKey("anual", 2024, None, "001")
    assert anual.filename == "anual_2024_001.parquet"


@pytest.mark.parametrize(
    "nome, motivo",
    [
        ("mensal_2024_057.parquet", "mensal exige mês"),
        ("anual_2024_01_057.parquet", "anual não aceita mês"),
        ("outro_2024_01_057.parquet", "prefixo fora do catálogo"),
        ("mensal_2024_01_999.parquet", "município fora do catálogo"),
        ("mensal_2024_01_057.csv", "não é Parquet"),
    ],
)
def test_nomes_invalidos_sao_rejeitados(nome, motivo):
    with pytest.raises(LegacyNameError):
        parse_filename(nome, CATALOG)


def test_scan_filtra_e_relata_rejeicoes(tmp_path):
    _parquet(tmp_path / "mensal_2024_01_057.parquet")
    _parquet(tmp_path / "mensal_2025_01_057.parquet")
    _parquet(tmp_path / "anual_2024_001.parquet")
    (tmp_path / "qualquer.txt").write_text("x")
    rejects = []

    itens = list(scan(tmp_path, CATALOG, rejects=rejects))
    assert len(itens) == 3
    assert [name for name, _ in rejects] == ["qualquer.txt"]

    apenas_2024 = list(
        scan(tmp_path, CATALOG, start_year=2024, end_year=2024)
    )
    assert sorted(item.key.filename for item in apenas_2024) == [
        "anual_2024_001.parquet",
        "mensal_2024_01_057.parquet",
    ]

    apenas_anual = list(scan(tmp_path, CATALOG, endpoint_prefix="anual"))
    assert [item.key.endpoint_prefix for item in apenas_anual] == ["anual"]


def test_inventory_compara_com_universo_planejado(tmp_path):
    _parquet(tmp_path / "mensal_2024_01_057.parquet")
    _parquet(tmp_path / "mensal_2024_02_001.parquet")
    _parquet(tmp_path / "anual_2024_001.parquet")
    (tmp_path / "lixo.bin").write_text("x")

    resumo = inventory(tmp_path, CATALOG, start_year=2024, end_year=2024)

    assert resumo["total"] == 3
    assert resumo["by_endpoint"] == {"anual": 1, "mensal": 2}
    assert resumo["planned"] == {"anual": 2, "mensal": 24}
    assert resumo["missing"] == {"anual": 1, "mensal": 22}
    assert resumo["rejects_total"] == 1
    texto = format_inventory(resumo)
    assert "TOTAL" in texto
    assert "Rejeitados: 1" in texto


def test_adotar_copia_manifesto_e_registra_estado(tmp_path):
    origem = tmp_path / "origem"
    origem.mkdir()
    _parquet(origem / "mensal_2024_01_057.parquet", rows=3)
    _parquet(origem / "anual_2024_001.parquet", rows=0)
    storage = LocalStorage(tmp_path / "lakehouse")
    repository = FakeRepository()

    resumo = adopt(
        origem,
        CATALOG,
        storage=storage,
        repository=repository,
        run_id="import__teste",
        workers=2,
    )

    assert resumo["processed"] == 2
    assert resumo["no_data"] == 1
    assert resumo["failed"] == 0

    estado = {record["endpoint_prefix"]: record for record in repository.records}
    assert estado["mensal"]["status"] == "extracted"
    assert estado["mensal"]["row_count"] == 3
    assert estado["anual"]["status"] == "no_data"

    parquet = tmp_path / "lakehouse/bronze/mensal/2024/mensal_2024_01_057.parquet"
    assert parquet.is_file()
    assert (parquet.stat().st_mode & 0o777) == 0o644
    manifesto = json.loads(
        parquet.with_name(parquet.name + ".manifest.json").read_text("utf-8")
    )
    assert manifesto["partition"]["filename"] == parquet.name
    assert manifesto["row_count"] == 3
    assert manifesto["content_sha256"] == estado["mensal"]["content_sha256"]
    assert manifesto["origin"]["extractor"] == "legado"


def test_limit_nao_varre_a_origem_inteira(tmp_path, monkeypatch):
    origem = tmp_path / "origem"
    origem.mkdir()
    for month in range(1, 6):
        _parquet(origem / f"mensal_2024_{month:02d}_057.parquet")

    from pipeline_tce.migration import legacy as modulo

    consumidos: list[str] = []
    real_scan = modulo.scan

    def espiao(*args, **kwargs):
        for item in real_scan(*args, **kwargs):
            consumidos.append(item.source.name)
            yield item

    monkeypatch.setattr(modulo, "scan", espiao)
    resumo = adopt(
        origem,
        CATALOG,
        storage=LocalStorage(tmp_path / "lakehouse"),
        repository=FakeRepository(),
        run_id="r1",
        limit=2,
    )

    assert resumo["processed"] == 2
    assert len(consumidos) == 2

    with pytest.raises(ValueError, match="limit"):
        adopt(
            origem,
            CATALOG,
            storage=LocalStorage(tmp_path / "lakehouse"),
            repository=FakeRepository(),
            run_id="r2",
            limit=0,
        )


def test_adocao_e_idempotente(tmp_path):
    origem = tmp_path / "origem"
    origem.mkdir()
    _parquet(origem / "mensal_2024_01_057.parquet", rows=2)
    storage = LocalStorage(tmp_path / "lakehouse")
    repository = FakeRepository()

    primeiro = adopt(
        origem, CATALOG, storage=storage, repository=repository, run_id="r1"
    )
    manifesto = tmp_path / "lakehouse/bronze/mensal/2024/mensal_2024_01_057.parquet.manifest.json"
    conteudo = manifesto.read_text("utf-8")

    segundo = adopt(
        origem, CATALOG, storage=storage, repository=repository, run_id="r2"
    )

    assert primeiro["processed"] == segundo["processed"] == 1
    assert manifesto.read_text("utf-8") == conteudo  # manifesto reaproveitado


def test_arquivo_corrompido_nao_interrompe_lote(tmp_path):
    origem = tmp_path / "origem"
    origem.mkdir()
    _parquet(origem / "mensal_2024_01_057.parquet", rows=1)
    (origem / "mensal_2024_02_057.parquet").write_bytes(b"nao e parquet")
    storage = LocalStorage(tmp_path / "lakehouse")

    resumo = adopt(
        origem,
        CATALOG,
        storage=storage,
        repository=FakeRepository(),
        run_id="r1",
    )

    assert resumo["processed"] == 1
    assert resumo["failed"] == 1
    assert resumo["failures"][0][0] == "mensal_2024_02_057.parquet"


def test_verify_detecta_diferenca_de_sha(tmp_path):
    origem = tmp_path / "origem"
    origem.mkdir()
    _parquet(origem / "mensal_2024_01_057.parquet", rows=2)
    storage = LocalStorage(tmp_path / "lakehouse")
    repository = FakeRepository()
    adopt(origem, CATALOG, storage=storage, repository=repository, run_id="r1")
    record = repository.records[0]

    ok = verify(
        CATALOG,
        storage=storage,
        repository=FakeRepository(
            [
                _snapshot(
                    PartitionKey("mensal", 2024, 1, "057"),
                    PartitionStatus.EXTRACTED,
                    sha=record["content_sha256"],
                )
            ]
        ),
        sample=1,
        seed=1,
    )
    assert ok["checked"] == 1
    assert ok["mismatches"] == []

    divergente = verify(
        CATALOG,
        storage=storage,
        repository=FakeRepository(
            [
                _snapshot(
                    PartitionKey("mensal", 2024, 1, "057"),
                    PartitionStatus.EXTRACTED,
                    sha="b" * 64,
                )
            ]
        ),
        sample=1,
        seed=1,
    )
    assert divergente["mismatches"][0]["partition"] == "mensal_2024_01_057.parquet"

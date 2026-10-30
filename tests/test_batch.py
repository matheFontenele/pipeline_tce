import pytest

from pipeline_tce.ingestion.batch import BatchPolicy, iter_partitions, run_batch
from pipeline_tce.ingestion.catalog import Endpoint, Municipality


def _endpoint(prefix, frequency="mensal"):
    return Endpoint(
        prefix.title(),
        f"https://api-dados-abertos.tce.ce.gov.br/sim/{prefix}",
        prefix,
        frequency,
        ("codigo_municipio",),
    )


ENDPOINTS = {"mensal": _endpoint("mensal"), "anual": _endpoint("anual", "anual")}
MUNICIPALITIES = {
    "001": Municipality("001", "A"),
    "002": Municipality("002", "B"),
}


def test_iter_partitions_gera_matriz_mensal_e_anual():
    partitions = list(iter_partitions(ENDPOINTS, MUNICIPALITIES, 2024, 2024))

    assert len(partitions) == 12 * 2 + 2
    assert {p.endpoint.prefix for p in partitions} == {"mensal", "anual"}


def test_iter_partitions_filtra_endpoint_e_municipio():
    partitions = list(
        iter_partitions(
            ENDPOINTS,
            MUNICIPALITIES,
            2024,
            2024,
            endpoint_prefixes=["anual"],
            municipality_code="002",
        )
    )

    assert [p.filename for p in partitions] == ["anual_2024_002.parquet"]


def test_iter_partitions_rejeita_endpoint_desconhecido():
    with pytest.raises(ValueError, match="Endpoint não cadastrado"):
        list(
            iter_partitions(ENDPOINTS, MUNICIPALITIES, 2024, 2024, endpoint_prefixes=["x"])
        )


def test_run_batch_conta_status_e_reaproveita_particiones_existentes():
    def fake_extract(partition, output_dir, **kwargs):
        if partition.filename.endswith("_002.parquet"):
            return {"status": "already_present", "file": partition.filename, "rows": 10}
        return {"status": "downloaded", "file": partition.filename, "rows": 5}

    partitions = list(iter_partitions(ENDPOINTS, MUNICIPALITIES, 2024, 2024))
    summary = run_batch(
        partitions,
        "/tmp/nao-usado",
        policy=BatchPolicy(max_workers=2, progress_every_partitions=1000),
        extract=fake_extract,
    )

    assert summary["processed"] == len(partitions)
    assert summary["plan_complete"] is True
    assert summary["status_counts"] == {"downloaded": 13, "already_present": 13}
    assert summary["circuit_breaker_open"] is False


def test_run_batch_respeita_teto_de_particoes():
    calls = []

    def fake_extract(partition, output_dir, **kwargs):
        calls.append(partition.filename)
        return {"status": "downloaded", "file": partition.filename, "rows": 1}

    partitions = iter_partitions(ENDPOINTS, MUNICIPALITIES, 2024, 2024)
    summary = run_batch(
        partitions,
        "/tmp/nao-usado",
        policy=BatchPolicy(max_workers=1, max_partitions=4, progress_every_partitions=1000),
        extract=fake_extract,
    )

    assert len(calls) == 4
    assert summary["hit_partition_cap"] is True
    assert summary["plan_complete"] is False


def test_run_batch_abre_circuit_breaker_apos_falhas_consecutivas():
    def failing_extract(partition, output_dir, **kwargs):
        raise RuntimeError("falha simulada")

    partitions = iter_partitions(ENDPOINTS, MUNICIPALITIES, 2024, 2024)
    policy = BatchPolicy(
        max_workers=2,
        max_partitions=50,
        max_consecutive_failures=2,
        progress_every_partitions=1000,
    )
    summary = run_batch(partitions, "/tmp/nao-usado", policy=policy, extract=failing_extract)

    assert summary["circuit_breaker_open"] is True
    assert summary["processed"] < len(list(iter_partitions(ENDPOINTS, MUNICIPALITIES, 2024, 2024)))
    assert summary["status_counts"]["failed"] == summary["processed"]


def test_batch_policy_rejeita_limites_invalidos():
    with pytest.raises(ValueError):
        BatchPolicy(max_workers=0)
    with pytest.raises(ValueError):
        BatchPolicy(max_partitions=0)
    with pytest.raises(ValueError):
        BatchPolicy(max_consecutive_failures=0)

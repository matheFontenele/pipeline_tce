import pytest

from pipeline_tce.ingestion.catalog import Endpoint, Municipality
from pipeline_tce.ingestion.planning import build_partition
from pipeline_tce.state.models import (
    PartitionKey,
    PartitionStatus,
    ensure_transition,
    InvalidTransitionError,
)


def test_partition_key_usa_o_mesmo_nome_do_planejamento():
    endpoint = Endpoint(
        "Notas de Empenho",
        "https://api-dados-abertos.tce.ce.gov.br/sim/notas_empenhos",
        "notas_empenho",
        "mensal",
        ("exercicio_orcamento", "codigo_municipio", "data_referencia_doc"),
    )
    annual_endpoint = Endpoint(
        "Itens de Notas Fiscais",
        "https://api-dados-abertos.tce.ce.gov.br/sim/itens_notas_fiscais",
        "itens_notas_fiscais",
        "anual",
        ("exercicio_orcamento", "codigo_municipio"),
    )
    municipality = Municipality("057", "FORTALEZA")

    mensal = build_partition(endpoint, 2024, 1, municipality)
    anual = build_partition(annual_endpoint, 2024, None, municipality)

    assert PartitionKey.from_partition(mensal).filename == mensal.filename
    assert PartitionKey.from_partition(anual).filename == anual.filename


def test_mes_anual_vira_zero_na_chave_persistida():
    assert PartitionKey("x", 2024, None, "057").storage_month == 0
    assert PartitionKey("x", 2024, 12, "057").storage_month == 12


@pytest.mark.parametrize(
    "args",
    [
        ("", 2024, 1, "057"),
        (" notas_empenho", 2024, 1, "057"),
        ("x", 2007, 1, "057"),
        ("x", 2024, 0, "057"),
        ("x", 2024, 13, "057"),
        ("x", 2024, 1, ""),
    ],
)
def test_chave_rejeita_valores_invalidos(args):
    with pytest.raises(ValueError):
        PartitionKey(*args)


def test_transicoes_do_ciclo_publicado_sao_permitidas():
    sequence = [
        PartitionStatus.PENDING,
        PartitionStatus.RUNNING,
        PartitionStatus.EXTRACTED,
        PartitionStatus.VALIDATED,
        PartitionStatus.PUBLISHED,
        PartitionStatus.PENDING,
    ]
    for current, target in zip(sequence, sequence[1:]):
        ensure_transition(current, target)


def test_transicao_invalida_falha_com_estados_no_erro():
    with pytest.raises(InvalidTransitionError, match="pending -> published"):
        ensure_transition(PartitionStatus.PENDING, PartitionStatus.PUBLISHED)


def test_artefato_extraido_ou_validado_so_volta_para_pending():
    for status in (PartitionStatus.EXTRACTED, PartitionStatus.VALIDATED):
        ensure_transition(status, PartitionStatus.PENDING)
        with pytest.raises(InvalidTransitionError):
            ensure_transition(status, PartitionStatus.RUNNING)


def test_reexecucao_do_mesmo_estado_e_idempotente():
    ensure_transition(PartitionStatus.RUNNING, PartitionStatus.RUNNING)

import json
from pathlib import Path

import pytest

from pipeline_tce.ingestion.catalog import (
    CatalogError,
    Endpoint,
    Municipality,
    load_endpoints,
    load_municipalities,
)
from pipeline_tce.ingestion.planning import build_partition


CATALOG = Path(__file__).parents[1] / "catalog"


def test_catalogo_portado_tem_21_endpoints_e_185_municipios():
    endpoints = load_endpoints(CATALOG / "endpoints.json")
    municipalities = load_municipalities(CATALOG / "municipios.json")

    assert len(endpoints) == 21
    assert len(municipalities) == 185
    assert municipalities["057"].name == "FORTALEZA"


def test_todos_endpoints_declarados_geram_parametros_de_particao():
    endpoints = load_endpoints(CATALOG / "endpoints.json")
    municipality = Municipality("057", "FORTALEZA")

    for endpoint in endpoints.values():
        month = 2 if endpoint.frequency == "mensal" else None
        partition = build_partition(endpoint, 2024, month, municipality)
        assert partition.filename.startswith(endpoint.prefix + "_2024_")
        assert "codigo_municipio" in partition.params
        assert set(endpoint.required_parameters).issubset(partition.params)


def test_endpoint_anual_gera_particao_e_parametros_sem_mes():
    endpoint = Endpoint(
        "Itens de Notas Fiscais",
        "https://api-dados-abertos.tce.ce.gov.br/sim/itens_notas_fiscais",
        "itens_notas_fiscais",
        "anual",
        ("exercicio_orcamento", "codigo_municipio"),
    )
    partition = build_partition(endpoint, 2025, None, Municipality("057", "FORTALEZA"))

    assert partition.filename == "itens_notas_fiscais_2025_057.parquet"
    assert partition.params == {
        "$format": "json",
        "$count": 1000,
        "$start_index": 0,
        "exercicio_orcamento": 202500,
        "codigo_municipio": "057",
    }


def test_endpoint_mensal_monta_janela_correta_para_fevereiro_bissexto():
    endpoint = Endpoint(
        "Bens Incorporados ao Patrimônio",
        "https://api-dados-abertos.tce.ce.gov.br/sim/bens_incorporados_patrimonio_municipio",
        "bens_incorporados_patrimonio_municipio",
        "mensal",
        ("codigo_municipio", "data_inicio", "data_fim"),
    )
    partition = build_partition(endpoint, 2024, 2, Municipality("057", "FORTALEZA"))

    assert partition.params["data_inicio"] == "20240201"
    assert partition.params["data_fim"] == "20240229"


def test_catalogo_rejeita_host_fora_da_api_tce(tmp_path):
    source = tmp_path / "endpoints.json"
    source.write_text(
        json.dumps(
            {
                "Endpoint": {
                    "endpoint": "https://evil.example/api",
                    "prefixo": "bad",
                    "frequencia": "mensal",
                    "parametros_obrigatorios": ["codigo_municipio", "data_referencia_doc"],
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(CatalogError, match="Host não permitido"):
        load_endpoints(source)


def test_catalogo_rejeita_url_com_host_tce_falsificado(tmp_path):
    source = tmp_path / "endpoints.json"
    source.write_text(
        json.dumps(
            {
                "Endpoint": {
                    "endpoint": "https://api-dados-abertos.tce.ce.gov.br@evil.example/sim/data",
                    "prefixo": "bad",
                    "frequencia": "mensal",
                    "parametros_obrigatorios": ["codigo_municipio", "data_referencia_doc"],
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(CatalogError, match="Host não permitido"):
        load_endpoints(source)


@pytest.mark.parametrize(
    ("month", "frequency"),
    [(None, "mensal"), (13, "mensal"), (1, "anual")],
)
def test_planejamento_rejeita_particao_incompativel(month, frequency):
    endpoint = Endpoint(
        "Teste",
        "https://api-dados-abertos.tce.ce.gov.br/sim/teste",
        "teste",
        frequency,
        ("codigo_municipio",),
    )

    with pytest.raises(ValueError):
        build_partition(endpoint, 2025, month, Municipality("057", "FORTALEZA"))

from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pyarrow.parquet as parquet
import pytest
import requests

from pipeline_tce.ingestion.catalog import Endpoint, Municipality
from pipeline_tce.ingestion.client import (
    RequestPacer,
    RetryPolicy,
    TceApiError,
    TceRateLimitError,
    extract_partition,
    retry_policy_from_env,
)
from pipeline_tce.ingestion.planning import build_partition


ENDPOINT = Endpoint(
    "Notas de Empenho",
    "https://api-dados-abertos.tce.ce.gov.br/sim/notas_empenhos",
    "notas_empenho",
    "mensal",
    ("exercicio_orcamento", "codigo_municipio", "data_referencia_doc"),
)
MUNICIPALITY = Municipality("057", "FORTALEZA")


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload or {"elements": []}
        self.headers = headers or {}

    def json(self):
        return self._payload


def _partition(year=2025, month=9):
    return build_partition(ENDPOINT, year, month, MUNICIPALITY)


def _fast_policy(**overrides):
    values = {
        "timeout_seconds": 1,
        "max_attempts": 3,
        "http_400_attempts": 2,
        "min_interval_seconds": 0,
        "http_400_backoff_seconds": 1,
        "http_429_backoff_seconds": 1,
        "connection_backoff_seconds": 1,
        "max_backoff_seconds": 1,
        "max_retry_after_seconds": 1,
    }
    values.update(overrides)
    return RetryPolicy(**values)


def _no_wait_pacer():
    return RequestPacer(0, sleep=lambda _: None)


def test_extrai_paginado_e_escreve_parquet_atomico(tmp_path):
    session = Mock()
    session.get.side_effect = [
        FakeResponse(payload={"elements": [{"exercicio_orcamento": 202500, "valor": 10}]}),
    ]

    result = extract_partition(
        _partition(), tmp_path, session=session, policy=_fast_policy(), pacer=_no_wait_pacer()
    )

    target = tmp_path / "notas_empenho_2025_09_057.parquet"
    frame = pd.read_parquet(target)
    assert result == {"status": "downloaded", "file": target.name, "rows": 1}
    assert frame.loc[0, "municipio_referencia"] == "FORTALEZA"
    assert list(tmp_path.glob("*.tmp")) == []
    assert session.get.call_args.kwargs["params"]["$start_index"] == 0


def test_pagina_proxima_ate_receber_menos_registros_que_o_limite(tmp_path, caplog):
    session = Mock()
    first_page = [{"exercicio_orcamento": 202500, "id": i} for i in range(2)]
    session.get.side_effect = [
        FakeResponse(payload={"elements": first_page}),
        FakeResponse(payload={"elements": [{"exercicio_orcamento": 202500, "id": 3}]}),
    ]
    policy = _fast_policy()
    partition = _partition()
    partition = partition.__class__(
        partition.endpoint,
        partition.year,
        partition.month,
        partition.municipality,
        {**partition.params, "$count": 2},
    )

    result = extract_partition(
        partition, tmp_path, session=session, policy=policy, pacer=_no_wait_pacer()
    )

    assert result["rows"] == 3
    assert session.get.call_count == 2
    assert session.get.call_args_list[1].kwargs["params"]["$start_index"] == 2
    assert "página=1" in caplog.text
    assert "página=2" in caplog.text
    assert "registros_acumulados=3" in caplog.text


def test_schema_novo_entre_paginas_falha_sem_substituir_arquivo_existente(tmp_path):
    partition = _partition()
    target = tmp_path / partition.filename
    original = b"parquet anterior corrompido"
    target.write_bytes(original)
    session = Mock()
    session.get.side_effect = [
        FakeResponse(payload={"elements": [{"exercicio_orcamento": 202500, "a": 1}]}),
        FakeResponse(payload={"elements": [{"exercicio_orcamento": 202500, "b": 2}]}),
    ]
    partition = partition.__class__(
        partition.endpoint,
        partition.year,
        partition.month,
        partition.municipality,
        {**partition.params, "$count": 1},
    )

    with pytest.raises(TceApiError, match="Schema mudou entre páginas"):
        extract_partition(
            partition,
            tmp_path,
            session=session,
            policy=_fast_policy(),
            pacer=_no_wait_pacer(),
        )

    assert target.read_bytes() == original
    assert list(tmp_path.glob("*.tmp")) == []


def test_http_429_respeita_retry_after_e_retorna_sucesso(tmp_path):
    session = Mock()
    session.get.side_effect = [
        FakeResponse(429, headers={"Retry-After": "3"}),
        FakeResponse(payload={"elements": [{"exercicio_orcamento": 202500}]}),
    ]
    pacer = Mock()
    pacer.wait.return_value = None
    pacer.pause.return_value = None

    result = extract_partition(
        _partition(), tmp_path, session=session, policy=_fast_policy(), pacer=pacer
    )

    assert result["status"] == "downloaded"
    assert pacer.pause.call_args.args[0] == 1  # limitado pelo máximo da política de teste


def test_http_429_exaurido_tem_erro_tipado_e_nao_escreve_arquivo(tmp_path):
    session = Mock()
    session.get.return_value = FakeResponse(429)

    with pytest.raises(TceRateLimitError):
        extract_partition(
            _partition(), tmp_path, session=session, policy=_fast_policy(), pacer=_no_wait_pacer()
        )

    assert list(tmp_path.iterdir()) == []


def test_http_400_tem_tentativa_limitada_e_pode_recuperar(tmp_path):
    session = Mock()
    session.get.side_effect = [
        FakeResponse(400),
        FakeResponse(payload={"elements": [{"exercicio_orcamento": 202500}]}),
    ]
    pacer = Mock()
    pacer.wait.return_value = None
    pacer.pause.return_value = None

    result = extract_partition(
        _partition(), tmp_path, session=session, policy=_fast_policy(), pacer=pacer
    )

    assert result["status"] == "downloaded"
    assert session.get.call_count == 2
    assert pacer.pause.call_args.args[0] == 1


def test_http_400_retorna_erro_sem_registrar_corpo_da_resposta(tmp_path, caplog):
    response = FakeResponse(400)
    response.text = "mensagem interna da API"
    session = Mock()
    session.get.return_value = response

    with pytest.raises(TceApiError, match="HTTP 400"):
        extract_partition(
            _partition(), tmp_path, session=session, policy=_fast_policy(), pacer=_no_wait_pacer()
        )

    assert "mensagem interna" not in caplog.text
    assert list(tmp_path.iterdir()) == []


def test_timeout_tem_retry_e_erros_nao_expoem_url(tmp_path, caplog):
    session = Mock()
    session.get.side_effect = [requests.Timeout("timeout"), FakeResponse(payload={"elements": []})]
    pacer = Mock()
    pacer.wait.return_value = None
    pacer.pause.return_value = None

    result = extract_partition(
        _partition(), tmp_path, session=session, policy=_fast_policy(), pacer=pacer
    )

    assert result["status"] == "no_data_cached"
    assert "https://" not in caplog.text


def test_ausencia_historica_grava_marcador_zero_colunas(tmp_path):
    session = Mock()
    session.get.return_value = FakeResponse(payload={"elements": []})

    result = extract_partition(
        _partition(2024, 9),
        tmp_path,
        session=session,
        policy=_fast_policy(),
        pacer=_no_wait_pacer(),
        today=date(2026, 1, 1),
    )

    path = tmp_path / result["file"]
    metadata = parquet.ParquetFile(path).metadata
    assert result["status"] == "no_data_cached"
    assert metadata.num_columns == 0


def test_ausencia_no_ano_corrente_nao_cria_marcador(tmp_path):
    session = Mock()
    session.get.return_value = FakeResponse(payload={"elements": []})

    result = extract_partition(
        _partition(2026, 9),
        tmp_path,
        session=session,
        policy=_fast_policy(),
        pacer=_no_wait_pacer(),
        today=date(2026, 10, 1),
    )

    assert result["status"] == "no_data_uncached"
    assert list(tmp_path.iterdir()) == []


def test_parquet_valido_e_marcador_negative_sao_checkpoints(tmp_path):
    path = tmp_path / _partition(2024, 9).filename
    pd.DataFrame().to_parquet(path, engine="pyarrow")
    session = Mock()

    result = extract_partition(
        _partition(2024, 9), tmp_path, session=session, policy=_fast_policy(), pacer=_no_wait_pacer()
    )

    assert result["status"] == "no_data_cached"
    session.get.assert_not_called()


def test_parquet_corrompido_e_reparado_sem_apagar_antes_da_resposta(tmp_path):
    partition = _partition()
    path = tmp_path / partition.filename
    path.write_bytes(b"parquet quebrado")
    session = Mock()
    session.get.return_value = FakeResponse(payload={"elements": [{"exercicio_orcamento": 202500}]})

    result = extract_partition(
        partition, tmp_path, session=session, policy=_fast_policy(), pacer=_no_wait_pacer()
    )

    assert result["status"] == "downloaded"
    assert parquet.ParquetFile(path).metadata.num_rows == 1


def test_parquet_corrompido_original_permanece_se_api_falhar(tmp_path):
    partition = _partition()
    path = tmp_path / partition.filename
    original = b"parquet quebrado"
    path.write_bytes(original)
    session = Mock()
    session.get.return_value = FakeResponse(400)

    with pytest.raises(TceApiError):
        extract_partition(
            partition, tmp_path, session=session, policy=_fast_policy(), pacer=_no_wait_pacer()
        )

    assert path.read_bytes() == original
    assert list(tmp_path.glob("*.tmp")) == []


def test_arquivo_valido_existente_nao_e_substituido(tmp_path):
    partition = _partition()
    path = tmp_path / partition.filename
    pd.DataFrame([{"sentinel": "keep"}]).to_parquet(path, engine="pyarrow")
    original = path.read_bytes()
    session = Mock()

    result = extract_partition(
        partition, tmp_path, session=session, policy=_fast_policy(), pacer=_no_wait_pacer()
    )

    assert result["status"] == "already_present"
    assert path.read_bytes() == original
    session.get.assert_not_called()


def test_request_pacer_espaca_chamadas_e_respeita_cooldown():
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    pacer = RequestPacer(2.0, clock=lambda: now[0], sleep=sleep)
    pacer.wait()
    pacer.wait()
    pacer.pause(5)
    pacer.wait()

    assert sleeps == [2.0, 5.0]


def test_configuracao_de_retry_invalida_falha_cedo():
    with pytest.raises(ValueError):
        retry_policy_from_env({"TCE_API_MIN_INTERVAL_SECONDS": "nan"})

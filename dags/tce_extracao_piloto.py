"""Extrai uma partição explicitamente informada para o volume isolado de piloto."""

from datetime import datetime, timedelta
import logging
import os
from pathlib import Path

from airflow.decorators import dag, task


@dag(
    dag_id="tce_extracao_piloto",
    description=(
        "Extrai uma única partição solicitada para área isolada; não valida nem publica dados."
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "tce-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(hours=1),
    },
    tags=["tce", "pilot", "external_api"],
)
def tce_extracao_piloto():
    @task(
        task_id="extrair_uma_particao",
        pool="tce_api_pool",
        execution_timeout=timedelta(hours=2),
        doc_md=(
            "Exige `endpoint_prefix`, `year`, `municipality_code` e `month` para endpoints mensais. "
            "Escreve apenas no volume Docker `airflow_pilot_data`; não acessa o repositório antigo, "
            "não valida contratos e não publica no banco de consulta."
        ),
    )
    def extrair_uma_particao(**context):
        from pipeline_tce.ingestion.catalog import load_endpoints, load_municipalities
        from pipeline_tce.ingestion.client import (
            RequestPacer,
            extract_partition,
            retry_policy_from_env,
        )
        from pipeline_tce.ingestion.planning import build_partition

        dag_run = context.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}
        if not isinstance(conf, dict):
            raise ValueError("A configuração do run deve ser um objeto JSON")
        required = {"endpoint_prefix", "year", "municipality_code"}
        missing = sorted(required - conf.keys())
        if missing:
            raise ValueError(f"Confirme os parâmetros obrigatórios: {', '.join(missing)}")

        endpoint_prefix = str(conf["endpoint_prefix"])
        try:
            if isinstance(conf["year"], bool) or isinstance(conf.get("month"), bool):
                raise ValueError
            year = int(conf["year"])
            municipality_code = str(conf["municipality_code"]).zfill(3)
            month_value = conf.get("month")
            month = int(month_value) if month_value is not None else None
        except (TypeError, ValueError) as exc:
            raise ValueError("Ano, mês ou código de município inválido") from exc

        endpoints = load_endpoints()
        municipalities = load_municipalities()
        if endpoint_prefix not in endpoints:
            raise ValueError(f"Endpoint não cadastrado: {endpoint_prefix}")
        if municipality_code not in municipalities:
            raise ValueError(f"Município não cadastrado: {municipality_code}")

        partition = build_partition(
            endpoints[endpoint_prefix], year, month, municipalities[municipality_code]
        )
        output_dir = Path(os.environ["TCE_PILOT_DATA_DIR"])
        policy = retry_policy_from_env()
        result = extract_partition(
            partition,
            output_dir,
            policy=policy,
            pacer=RequestPacer(policy.min_interval_seconds),
        )

        logging.info(
            "Resumo da partição: endpoint=%s ano=%s mês=%s município=%s "
            "status=%s linhas=%s arquivo=%s destino=piloto-isolado",
            endpoint_prefix,
            year,
            month if month is not None else "anual",
            municipality_code,
            result["status"],
            result["rows"],
            result["file"],
        )
        return result

    extrair_uma_particao()


tce_extracao_piloto()

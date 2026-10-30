"""Extrai um lote limitado de partições para o volume isolado do piloto."""

from datetime import datetime, timedelta
import logging
import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param


@dag(
    dag_id="tce_extracao_lote",
    description=(
        "Extrai um lote limitado para área isolada, com progresso e retomada; "
        "não valida nem publica dados."
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "endpoint_prefixes": Param(
            "",
            type="string",
            description=(
                "Prefixos separados por vírgula. Vazio usa os 21 endpoints do catálogo."
            ),
        ),
        "start_year": Param(
            2024,
            type="integer",
            minimum=2008,
            maximum=2100,
            description="Primeiro ano do lote.",
        ),
        "end_year": Param(
            2024,
            type="integer",
            minimum=2008,
            maximum=2100,
            description="Último ano do lote; endpoints mensais incluem os 12 meses.",
        ),
        "municipality_code": Param(
            "057",
            type="string",
            pattern="^$|^[0-9]{3}$",
            description="Código de três dígitos; vazio abrange os 185 municípios.",
        ),
        "max_partitions": Param(
            500,
            type="integer",
            minimum=1,
            maximum=5000,
            description="Teto de partições submetidas nesta execução.",
        ),
        "max_workers": Param(
            3,
            type="integer",
            minimum=1,
            maximum=5,
            description="Requisições simultâneas dentro da task.",
        ),
    },
    default_args={
        "owner": "tce-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(hours=1),
    },
    tags=["tce", "pilot", "batch", "external_api"],
)
def tce_extracao_lote():
    @task(
        task_id="extrair_lote",
        pool="tce_api_pool",
        execution_timeout=timedelta(hours=6),
        doc_md=(
            "Processa endpoints × anos × meses × municípios até `max_partitions`. "
            "Partições já válidas são puladas, então repetir retoma apenas o que falta. "
            "Interrompe após 15 falhas consecutivas. Grava somente no volume Docker "
            "`airflow_pilot_data`; não valida contratos e não publica."
        ),
    )
    def extrair_lote(**context):
        from pipeline_tce.ingestion.batch import BatchPolicy, iter_partitions, run_batch
        from pipeline_tce.ingestion.catalog import load_endpoints, load_municipalities
        from pipeline_tce.ingestion.client import retry_policy_from_env

        params = dict(context.get("params") or {})
        run = context.get("dag_run")
        conf = getattr(run, "conf", None) or {}
        if not isinstance(conf, dict):
            raise ValueError("A configuração do run deve ser um objeto JSON")
        params.update(conf)

        raw_prefixes = str(params.get("endpoint_prefixes") or "")
        prefixes = [item.strip() for item in raw_prefixes.split(",") if item.strip()]
        if isinstance(params["start_year"], bool) or isinstance(params["end_year"], bool):
            raise ValueError("Ano inválido")
        start_year = int(params["start_year"])
        end_year = int(params["end_year"])
        municipality_code = str(params.get("municipality_code") or "").strip() or None

        endpoints = load_endpoints()
        municipalities = load_municipalities()
        policy = BatchPolicy(
            max_workers=int(params["max_workers"]),
            max_partitions=int(params["max_partitions"]),
        )
        partitions = iter_partitions(
            endpoints,
            municipalities,
            start_year,
            end_year,
            endpoint_prefixes=prefixes or None,
            municipality_code=municipality_code,
        )
        logging.info(
            "Lote iniciado: endpoints=%s anos=%s-%s município=%s teto=%s workers=%s",
            ",".join(prefixes) if prefixes else "todos",
            start_year,
            end_year,
            municipality_code or "todos",
            policy.max_partitions,
            policy.max_workers,
        )
        summary = run_batch(
            partitions,
            Path(os.environ["TCE_PILOT_DATA_DIR"]),
            policy=policy,
            retry_policy=retry_policy_from_env(),
        )
        if summary["hit_partition_cap"]:
            logging.warning(
                "Teto de %s partições atingido; o lote tem mais itens pendentes. "
                "Repita a execução para continuar de onde parou.",
                policy.max_partitions,
            )
        return summary

    extrair_lote()


tce_extracao_lote()

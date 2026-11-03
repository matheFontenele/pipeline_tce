"""Exercita o ciclo estado -> extração -> manifesto em volume isolado.

Esta DAG não publica para consumidores e não substitui a DAG piloto. Ela existe
para validar, ponta a ponta, a fundação de estado e armazenamento antes de
qualquer migração do caminho atual.
"""

from datetime import datetime, timedelta
import logging
import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param


@dag(
    dag_id="tce_pipeline_particao",
    description=(
        "Extrai uma partição com estado, storage e manifesto; não publica para consumidores."
    ),
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "endpoint_prefix": Param(
            "notas_empenho",
            type="string",
            minLength=1,
            description="Prefixo do endpoint no catálogo versionado.",
        ),
        "year": Param(
            2024,
            type="integer",
            minimum=2008,
            maximum=2100,
            description="Ano da partição.",
        ),
        "month": Param(
            1,
            type="integer",
            minimum=1,
            maximum=12,
            description="Mês (1–12); ignorado por endpoints anuais.",
        ),
        "municipality_code": Param(
            "057",
            type="string",
            pattern="^[0-9]{3}$",
            description="Código de três dígitos do município.",
        ),
    },
    default_args={
        "owner": "tce-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(hours=1),
    },
    tags=["tce", "pipeline", "state", "external_api"],
)
def tce_pipeline_particao():
    @task(
        task_id="executar_particao",
        pool="tce_api_pool",
        execution_timeout=timedelta(hours=2),
        doc_md=(
            "Registra `pending`, marca `running` e grava Parquet + manifesto em "
            "`airflow_pipeline_data`. Ausência histórica vira `no_data`; falha vira "
            "`failed` e não publica arquivo parcial."
        ),
    )
    def executar_particao(**context):
        from pipeline_tce.ingestion.catalog import load_endpoints, load_municipalities
        from pipeline_tce.ingestion.client import RequestPacer, retry_policy_from_env
        from pipeline_tce.ingestion.planning import build_partition
        from pipeline_tce.pipeline.partition import run_partition
        from pipeline_tce.state.repository import PartitionRepository
        from pipeline_tce.storage.local import LocalStorage

        dag_run = context.get("dag_run")
        conf = getattr(dag_run, "conf", None) or {}
        if not isinstance(conf, dict):
            raise ValueError("A configuração do run deve ser um objeto JSON")
        run_params = dict(context.get("params") or {})
        run_params.update(conf)
        required = {"endpoint_prefix", "year", "municipality_code"}
        missing = sorted(required - run_params.keys())
        if missing:
            raise ValueError(f"Confirme os parâmetros obrigatórios: {', '.join(missing)}")

        endpoint_prefix = str(run_params["endpoint_prefix"])
        try:
            if isinstance(run_params["year"], bool) or isinstance(run_params.get("month"), bool):
                raise ValueError
            year = int(run_params["year"])
            municipality_code = str(run_params["municipality_code"]).zfill(3)
            month_value = run_params.get("month")
            month = int(month_value) if month_value is not None else None
        except (TypeError, ValueError) as exc:
            raise ValueError("Ano, mês ou código de município inválido") from exc

        endpoints = load_endpoints()
        municipalities = load_municipalities()
        if endpoint_prefix not in endpoints:
            raise ValueError(f"Endpoint não cadastrado: {endpoint_prefix}")
        if municipality_code not in municipalities:
            raise ValueError(f"Município não cadastrado: {municipality_code}")

        endpoint = endpoints[endpoint_prefix]
        if endpoint.frequency == "anual":
            month = None

        partition = build_partition(
            endpoint, year, month, municipalities[municipality_code]
        )
        data_dir = Path(os.environ["TCE_PIPELINE_DATA_DIR"])
        policy = retry_policy_from_env()

        result = run_partition(
            partition,
            repository=PartitionRepository.from_env(),
            storage=LocalStorage(data_dir),
            run_id=context["run_id"],
            staging_dir=data_dir / "_staging",
            policy=policy,
            pacer=RequestPacer(policy.min_interval_seconds),
        )

        logging.info(
            "Resumo da partição: endpoint=%s ano=%s mês=%s município=%s "
            "estado=%s linhas=%s destino=volume-isolado",
            endpoint_prefix,
            year,
            month if month is not None else "anual",
            municipality_code,
            result.status,
            result.rows,
        )
        return result.as_dict()

    @task(task_id="conferir_particao", execution_timeout=timedelta(minutes=10))
    def conferir_particao(**context):
        from pipeline_tce.pipeline.partition import verify_partition
        from pipeline_tce.state.models import PartitionKey
        from pipeline_tce.state.repository import PartitionRepository
        from pipeline_tce.storage.local import LocalStorage

        previous = context["ti"].xcom_pull(task_ids="executar_particao")
        if not isinstance(previous, dict) or not isinstance(previous.get("key"), dict):
            raise AirflowException("Resultado da extração ausente ou inválido")
        key = PartitionKey(**previous["key"])

        verification = verify_partition(
            key,
            repository=PartitionRepository.from_env(),
            storage=LocalStorage(Path(os.environ["TCE_PIPELINE_DATA_DIR"])),
        )
        logging.info(
            "Conferência da partição: arquivo=%s estado=%s linhas=%s",
            key.filename,
            verification["status"],
            verification["rows"],
        )
        return verification

    executar_particao() >> conferir_particao()


tce_pipeline_particao()

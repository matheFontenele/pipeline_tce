"""Atualiza diariamente a janela de meses recentes no lakehouse.

Reextrai sempre o mês corrente e os anteriores pedidos em ``months_back``, para
todos os municípios, porque os dados do mês em curso mudam todo dia e o mês
recém-fechado ainda recebe revisão. O restante do histórico não é tocado: só
volta a ser extraído por ``request_reprocess`` explícito.

Esta DAG substitui a extração recorrente do extrator anterior. Ela grava no
lakehouse com estado, storage e manifesto, mas não valida contratos nem publica
para consumidores.
"""

from datetime import date, datetime, timedelta
import logging
import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param


@dag(
    dag_id="tce_atualizacao_diaria",
    description=(
        "Reextrai mês corrente e anteriores (todos os municípios) com estado, "
        "storage e manifesto; não publica para consumidores."
    ),
    schedule="30 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "tce",
        "retries": 1,
        "retry_delay": timedelta(minutes=30),
    },
    params={
        "months_back": Param(
            2,
            type="integer",
            minimum=1,
            maximum=12,
            description=(
                "Quantidade de meses reextraídos, contando o mês corrente "
                "(2 = mês corrente + mês anterior)."
            ),
        ),
        "max_workers": Param(
            2,
            type="integer",
            minimum=1,
            maximum=4,
            description="Requisições simultâneas; o pacer da API continua valendo.",
        ),
        "endpoint_prefixes": Param(
            "",
            type="string",
            description=(
                "Prefixos separados por vírgula. Vazio usa os 21 endpoints do catálogo."
            ),
        ),
        "max_partitions": Param(
            0,
            type="integer",
            minimum=0,
            description="Teto de partições nesta execução; 0 não aplica teto.",
        ),
    },
    tags=["tce", "pipeline", "state", "external_api", "diaria"],
)
def tce_atualizacao_diaria():
    @task(
        task_id="atualizar_janela",
        pool="tce_api_pool",
        execution_timeout=timedelta(hours=7),
        doc_md=(
            "Reextrai a janela de meses recentes com `refresh=True`: partições "
            "`extracted`/`no_data` da janela voltam a `pending` e são substituídas "
            "só depois de uma resposta completa. Ausência no ano corrente não vira "
            "cache negativo e é retentada na execução seguinte."
        ),
    )
    def atualizar_janela(**context):
        from pipeline_tce.ingestion.catalog import load_endpoints, load_municipalities
        from pipeline_tce.ingestion.client import RequestPacer, retry_policy_from_env
        from pipeline_tce.pipeline.window import (
            WindowPolicy,
            months_window,
            run_window,
            window_partitions,
        )
        from pipeline_tce.state.repository import PartitionRepository
        from pipeline_tce.storage.local import LocalStorage

        run = context.get("dag_run")
        conf = getattr(run, "conf", None) or {}
        if not isinstance(conf, dict):
            raise ValueError("A configuração do run deve ser um objeto JSON")
        params = dict(context.get("params") or {})
        params.update(conf)

        for nome in ("months_back", "max_workers", "max_partitions"):
            if isinstance(params.get(nome), bool):
                raise ValueError(f"Parâmetro inválido: {nome}")
        months_back = int(params.get("months_back") or 2)
        max_workers = int(params.get("max_workers") or 2)
        max_partitions = int(params.get("max_partitions") or 0)
        raw_prefixes = str(params.get("endpoint_prefixes") or "")
        prefixes = [item.strip() for item in raw_prefixes.split(",") if item.strip()]

        window = months_window(date.today(), months_back)
        endpoints = load_endpoints()
        municipalities = load_municipalities()
        policy = WindowPolicy(
            max_workers=max_workers,
            max_partitions=max_partitions or None,
        )
        retry_policy = retry_policy_from_env()
        data_dir = Path(os.environ["TCE_PIPELINE_DATA_DIR"])

        logging.info(
            "Atualização diária: janela=%s endpoints=%s municípios=%s workers=%s teto=%s",
            ",".join(f"{year}-{month:02d}" for year, month in window),
            ",".join(prefixes) if prefixes else "todos",
            len(municipalities),
            policy.max_workers,
            policy.max_partitions or "sem teto",
        )

        summary = run_window(
            window_partitions(
                endpoints,
                municipalities,
                window,
                endpoint_prefixes=prefixes or None,
            ),
            repository=PartitionRepository.from_env(),
            storage=LocalStorage(data_dir),
            run_id=context["run_id"],
            staging_dir=data_dir / "_staging",
            policy=policy,
            retry_policy=retry_policy,
            pacer=RequestPacer(retry_policy.min_interval_seconds),
            refresh=True,
        )

        logging.info(
            "Resumo da janela: processos=%s extraídos=%s vazios=%s sem_dados=%s "
            "falhas=%s circuito_aberto=%s segundos=%s",
            summary["processed"],
            summary["extracted"],
            summary["no_data"],
            summary["pending_data"],
            summary["failed"],
            summary["circuit_open"],
            summary["seconds"],
        )
        if summary["circuit_open"]:
            raise AirflowFailException(
                "Atualização interrompida por falhas consecutivas; as partições "
                "concluídas permanecem salvas e a próxima execução retoma."
            )
        if not summary["plan_complete"]:
            logging.warning(
                "Teto de %s partições atingido; ainda falta parte da janela. "
                "Repita a execução para continuar de onde parou.",
                policy.max_partitions,
            )
        return summary

    atualizar_janela()


tce_atualizacao_diaria()

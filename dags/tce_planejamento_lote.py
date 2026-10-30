"""Prévia do catálogo por intervalo; não consulta API nem altera arquivos."""

from datetime import datetime
import logging
import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param


@dag(
    dag_id="tce_planejamento_lote",
    description="Conta partições do catálogo num intervalo, sem extrair dados.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "start_year": Param(
            2008,
            type="integer",
            minimum=2008,
            maximum=2100,
            description="Primeiro ano do intervalo de planejamento.",
        ),
        "end_year": Param(
            2026,
            type="integer",
            minimum=2008,
            maximum=2100,
            description="Último ano do intervalo; endpoints mensais incluem os 12 meses.",
        ),
    },
    tags=["tce", "planning", "preview"],
)
def tce_planejamento_lote():
    @task(task_id="contar_particoes")
    def contar_particoes(**context):
        from pipeline_tce.ingestion.catalog import load_endpoints, load_municipalities
        from pipeline_tce.ingestion.planning import plan_catalog_range

        params = dict(context.get("params") or {})
        run = context.get("dag_run")
        conf = getattr(run, "conf", None) or {}
        if not isinstance(conf, dict):
            raise ValueError("A configuração do run deve ser um objeto JSON")
        params.update(conf)

        if isinstance(params["start_year"], bool) or isinstance(params["end_year"], bool):
            raise ValueError("Ano inválido")
        start_year = int(params["start_year"])
        end_year = int(params["end_year"])

        output_dir = Path(os.environ["TCE_PILOT_DATA_DIR"])
        municipalities = load_municipalities()
        summary = plan_catalog_range(
            load_endpoints(),
            municipalities,
            start_year,
            end_year,
            output_dir,
        )
        totals = {
            key: sum(count[key] for count in summary.values())
            for key in ("total", "existing", "to_process")
        }
        logging.info(
            "PRÉVIA (sem chamadas à API): anos=%s-%s endpoints=%s municípios=%s "
            "total=%s com_arquivo_existente=%s candidatos=%s",
            start_year,
            end_year,
            len(summary),
            len(municipalities),
            totals["total"],
            totals["existing"],
            totals["to_process"],
        )
        for prefix, counts in summary.items():
            logging.info(
                "PRÉVIA endpoint=%s total=%s com_arquivo_existente=%s candidatos=%s",
                prefix,
                counts["total"],
                counts["existing"],
                counts["to_process"],
            )
        return {
            "start_year": start_year,
            "end_year": end_year,
            "endpoints": len(summary),
            "municipalities": len(municipalities),
            **totals,
        }

    contar_particoes()


tce_planejamento_lote()

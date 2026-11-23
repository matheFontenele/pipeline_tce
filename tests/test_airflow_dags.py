from pathlib import Path

from airflow.models import DagBag


def test_airflow_dags_carregam_sem_erro_de_importacao():
    repository = Path(__file__).parents[1]
    dag_bag = DagBag(dag_folder=str(repository / "dags"), include_examples=False)

    assert dag_bag.import_errors == {}
    assert "tce_extracao_piloto" in dag_bag.dags
    assert "tce_planejamento_lote" in dag_bag.dags
    assert "tce_extracao_lote" in dag_bag.dags
    assert "tce_pipeline_particao" in dag_bag.dags
    assert "tce_atualizacao_diaria" in dag_bag.dags
    assert "platform_smoke_test" not in dag_bag.dags
    pilot = dag_bag.dags["tce_extracao_piloto"]
    assert pilot.schedule_interval is None
    assert pilot.max_active_runs == 1
    assert {
        name: param.value
        for name, param in pilot.params.items()
    } == {
        "endpoint_prefix": "notas_empenho",
        "year": 2024,
        "month": 1,
        "municipality_code": "057",
    }
    task = pilot.get_task("extrair_uma_particao")
    assert task.pool == "tce_api_pool"
    batch = dag_bag.dags["tce_extracao_lote"]
    assert batch.max_active_runs == 1
    assert batch.get_task("extrair_lote").pool == "tce_api_pool"
    pipeline = dag_bag.dags["tce_pipeline_particao"]
    assert pipeline.schedule_interval is None
    assert pipeline.max_active_runs == 1
    assert pipeline.get_task("executar_particao").pool == "tce_api_pool"
    assert pipeline.get_task("conferir_particao").upstream_task_ids == {
        "executar_particao"
    }
    diaria = dag_bag.dags["tce_atualizacao_diaria"]
    assert diaria.schedule_interval == "30 2 * * *"
    assert diaria.max_active_runs == 1
    assert diaria.get_task("atualizar_janela").pool == "tce_api_pool"
    diaria_params = {
        name: getattr(param, "value", param)
        for name, param in diaria.params.items()
    }
    assert diaria_params == {
        "months_back": 2,
        "max_workers": 2,
        "endpoint_prefixes": "",
        "max_partitions": 0,
    }

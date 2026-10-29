from pathlib import Path

from airflow.models import DagBag


def test_airflow_dags_carregam_sem_erro_de_importacao():
    repository = Path(__file__).parents[1]
    dag_bag = DagBag(dag_folder=str(repository / "dags"), include_examples=False)

    assert dag_bag.import_errors == {}
    assert "tce_extracao_piloto" in dag_bag.dags
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

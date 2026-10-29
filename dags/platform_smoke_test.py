"""DAG manual e sem efeitos colaterais para validar o Airflow da plataforma."""

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="platform_smoke_test",
    description="Verifica execução básica do Airflow sem acessar dados do TCE.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["platform", "smoke-test"],
)
def platform_smoke_test():
    @task(task_id="confirmar_scheduler_e_executor")
    def confirmar_scheduler_e_executor():
        import airflow

        resultado = {
            "status": "ok",
            "airflow_version": airflow.__version__,
            "message": "Scheduler e executor executaram a DAG de teste.",
        }
        print(resultado["message"])
        return resultado

    confirmar_scheduler_e_executor()


platform_smoke_test()

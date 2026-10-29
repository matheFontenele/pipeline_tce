# Airflow de origem — referência de inspeção

Inspecionado em 2026-10-29 no caminho indicado pelo responsável: `/home/matheus/automacao_api_tse/src/orchestration/airflow/` e serviços correspondentes do Compose da raiz. Este documento é uma linha de base observada, não uma especificação para copiar nem autorização de implantação.

## Configuração encontrada

- Dockerfile: `apache/airflow:2.10.4-python3.10`; instala `src/orchestration/airflow/requirements.txt`, que inclui pandas, requests, pyarrow, DuckDB, Streamlit e bibliotecas não necessariamente necessárias ao Airflow independente.
- Compose: `LocalExecutor`; PostgreSQL 16 para metadados; `airflow-init` executa `airflow db migrate` e tenta criar usuário `admin` com senha `admin`, aceitando falha com `|| true`.
- Webserver publicado em `8085:8080`, sem bind restrito a loopback; scheduler, webserver e init montam o repositório inteiro em `/app` e compartilham logs.
- A configuração do banco de metadados usa `airflow/airflow` como credenciais fixas. O Compose também injeta credencial administrativa do banco de consulta para a DAG de origem.
- Fuso `America/Fortaleza`, `LocalExecutor`, exemplos desabilitados e healthchecks estão configurados.

## DAG principal observada

`src/orchestration/airflow/dags/tce_extracao_mensal.py` declara `tce_extracao_mensal`, agenda diariamente, limita a uma execução ativa e define três retries com backoff exponencial até seis horas. A cadeia verifica espaço, calcula período desde 2008, chama `worker_extrator`, executa validação de Parquets e atualiza views/consulta.

Pontos que impedem cópia direta:

- Acrescenta `PROJECT_ROOT` e `PROJECT_ROOT/src` ao `sys.path`, importa `worker_extrator`/`src.quality` e usa caminhos do checkout antigo.
- Percorre um horizonte histórico amplo na rotina diária (ainda que o extrator pule arquivos existentes), em vez de consultar estado persistido de partições pendentes.
- A validação registra relatório, mas declara explicitamente que não bloqueia atualização nesta fase.
- Refresh é chamado com credencial administrativa; a nova plataforma deverá isolar credenciais e não entregar admin a consumidores.
- A guarda de disco verifica o volume local do projeto, que não representa automaticamente um backend de storage compartilhado.

## Consequência para a plataforma nova

O Compose novo começa separado, com volume apenas para metadados/logs de desenvolvimento, imagem configurável, credenciais obrigatórias e porta web ligada a `127.0.0.1` por padrão. Nenhum código DAG da origem foi copiado, e nenhuma extração está habilitada. A versão 2.10.4/Python 3.10 é apenas uma referência histórica; a compatibilidade e suporte no destino precisam ser confirmados.

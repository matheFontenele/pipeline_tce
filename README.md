# pipeline_tce

Plataforma independente para orquestrar a extração, validação e publicação dos dados abertos do TCE-CE. O Airflow agenda e monitora o pipeline; não é o datalake. Os consumidores não executam extração e terão acesso de leitura por uma interface documentada.

## Estado do projeto

Esta é a fundação de desenvolvimento. Há uma DAG manual para extrair **uma partição** à área local isolada `airflow_pilot_data`; ela não valida nem publica os dados. Storage compartilhado de produção, banco de consulta, rede/TLS, recursos, versões suportadas e cutover permanecem pendentes. **Não use para ingestão de produção nem migre dados.**

Consulte [`docs/architecture-plan.md`](docs/architecture-plan.md) para arquitetura provisória, decisões, riscos e etapas de migração; [`docs/airflow-orchestration.md`](docs/airflow-orchestration.md) para granularidade, estados, pools, logs e plano de implementação do Airflow; e [`docs/data-pipeline-roadmap.md`](docs/data-pipeline-roadmap.md) para a evolução de extrator para pipeline completo (estado, storage, contratos, transformação e consulta). As referências ao Airflow antigo são históricas e não aprovam a mesma configuração para produção.

## Desenvolvimento local

Requisitos: Docker Engine, Docker Compose v2 e `openssl`. Não é necessário Python instalado no host.

```bash
bash scripts/configurar-ambiente.sh
```

O script cria `.env` apenas se ainda não existir, gera segredos locais e restringe as permissões do arquivo. Ele imprime a senha inicial do usuário admin uma vez para permitir o login; guarde-a em um local seguro. Nunca compartilhe nem versione `.env`. Se o `.env` já existir, o script não o sobrescreve: revise suas variáveis obrigatórias manualmente.

Antes de iniciar, confira que não há valores `REPLACE_*`:

```bash
grep -n 'REPLACE_' .env
docker compose config --quiet
```

Se o `grep` encontrar valores, substitua-os por valores válidos. Depois:

```bash
docker compose up -d --build
```

A interface local fica em `http://127.0.0.1:8085` por padrão. O bind é configurável via `AIRFLOW_WEBSERVER_BIND`; não altere para `0.0.0.0` sem uma configuração aprovada de rede e proxy/TLS. O Compose contém apenas o banco de metadados isolado do Airflow; não monta o repositório de origem nem dados TCE.

O serviço `airflow-init` aplica migrações, cria o usuário admin apenas se ausente e configura `tce_api_pool` com um slot (limite serial conservador). Pode consultar a UI em Admin → Pools; não aumente os slots sem medir capacidade e confirmar limites da API.

A DAG manual `tce_extracao_piloto` aceita **uma partição por execução** e escreve somente no volume Docker `airflow_pilot_data`, isolado do projeto de origem e não compartilhado com consumidores. O formulário **Trigger DAG** apresenta quatro campos preenchidos inicialmente com `notas_empenho`, ano `2024`, mês `1` e município `057`; revise-os antes de confirmar, pois clicar em disparar com esses valores executa essa partição e pode consultar a API. Para solicitar outra partição, informe os valores correspondentes; pela CLI, use `--conf` com o JSON abaixo:

```json
{
  "endpoint_prefix": "notas_empenho",
  "year": 2024,
  "month": 1,
  "municipality_code": "057"
}
```

Para estimar busca em massa sem chamar a API, use a DAG manual `tce_planejamento_lote`, informando o intervalo de anos. Ela inclui todos os endpoints e municípios; para endpoints mensais, considera os 12 meses de cada ano. O padrão é 2008–2026. O resultado distingue total, partições com arquivo já existente e candidatas, inclusive um resumo por endpoint. A existência não garante que o arquivo seja válido. Esta DAG é somente prévia: não baixa, valida nem altera arquivos.

Para extrair em lote, use `tce_extracao_lote`. Os padrões são conservadores: todos os endpoints, ano `2024`, município `057`, teto de **500 partições** e **3 requisições simultâneas**. O lote inclui apenas o que ainda não existe; repetir a execução retoma de onde parou. Ele interrompe após 15 falhas consecutivas (circuit breaker) e registra progresso a cada 100 partições ou 15 s, com taxa e ETA. Aumentar `max_partitions` ou remover o filtro de município amplia o tempo e o volume; faça isso só depois de medir com um lote pequeno. Continua sem validação contratual e sem publicação.

O prefixo deve existir no catálogo, município deve estar no catálogo oficial e endpoints anuais não recebem `month`. Os limites HTTP começam serializados: uma chamada por vez no pool, intervalo mínimo de 1,5 s, timeout de 90 s, retry/backoff para HTTP 400/429 e falhas transitórias. O cliente não imprime corpo da API nem query string nos logs. A repetição preserva Parquet válido; arquivo corrompido só é substituído depois de uma resposta completa e gravação atômica. Ausência histórica gera marcador Parquet de zero colunas; ausência no ano corrente não é cacheada.

O volume `airflow_pilot_data` tem limite preventivo de ocupação em disco configurável por `TCE_PILOT_MAX_DISK_USAGE` (padrão `0.90`). O cliente verifica o filesystem antes das chamadas e antes de gravar páginas; ao atingir 90%, interrompe a partição e o lote sem retry automático. Parquets finais concluídos são preservados; o arquivo temporário da partição em curso é descartado. Esse limite mede disco do filesystem, não RAM.

Essa DAG é um **piloto de extração**, não é validação contratual/qualidade e não publica dados. Não use para cargas amplas. Para inspeção do artefato do piloto, veja `/opt/airflow/pilot-data` dentro do scheduler; não copie esse volume como datalake de produção.

### Pipeline com estado e manifesto

A DAG manual `tce_pipeline_particao` exercita a fundação da Fase 1: registra a partição como `pending`, marca `running`, grava o Parquet no lakehouse (layout `bronze/<endpoint>/<ano>/`), gera `*.manifest.json` com sha256/linhas/colunas e marca `extracted` ou `no_data`. Uma segunda task confere que estado, manifesto e arquivo concordam. Repetir a execução de uma partição já extraída **não** reconsulta a API nem sobrescreve o artefato: a task retorna `skipped` e a conferência continua validando a versão existente; para reextrair é preciso solicitar reprocessamento no estado (`extracted`/`validated`/`published`/`no_data` -> `pending`). Ela **não publica** e não altera o piloto.

#### Lakehouse e consumo

O lakehouse é um diretório do host montado no scheduler, não um volume gerenciado pelo Docker: `${TCE_LAKEHOUSE_DIR:-./lakehouse}` -> `/opt/airflow/pipeline-data`. Artigos são gravados com modo `0644` para leitura por aplicações que rodam com outro usuário no mesmo host.

```text
Host:              <TCE_LAKEHOUSE_DIR>/bronze/<endpoint>/<ano>/<arquivo>.parquet
Container Airflow: /opt/airflow/pipeline-data/bronze/<endpoint>/<ano>/<arquivo>.parquet
Manifesto:         <mesmo caminho>.manifest.json
Staging interno:   <TCE_LAKEHOUSE_DIR>/_staging  (não consumir)
```

Contrato para o app consumidor:

- leia **somente** `bronze/**`; `_staging` e arquivos `.tmp` são internos;
- abra o Parquet apenas se o `.manifest.json` ao lado existir e tiver o mesmo `content_sha256`, `row_count` e `columns` esperados;
- trate o manifesto como commit: Parquet sem manifesto é escrita em andamento ou incompleta;
- monte em modo somente leitura quando o app estiver em container (`:ro`);
- o app deve executar como usuário com permissão de leitura na pasta (dono do diretório, grupo ou ACL); se rodar como outro usuário no host, libere a travessia do diretório pai com `setfacl -m u:<usuario>:x /home/controle-interno`.

O estado ainda é `extracted` (bronze conferido); validação contratual/qualidade e publicação ficam para a Fase 2 do roadmap. O contrato detalhado para o app consumidor está em [`docs/consumer-access.md`](docs/consumer-access.md).

Requisitos:

- `TCE_STATE_DB_PASSWORD` no `.env` (sem valor padrão);
- `airflow-init` provisiona papel `pipeline_state`, banco `pipeline_state` e schema de forma idempotente;
- se a senha estiver ausente, o init apenas avisa e a DAG falha com mensagem clara ao tentar executar.

O lakehouse (`TCE_LAKEHOUSE_DIR`) é separado de `airflow_pilot_data`, que permanece como volume isolado do lote antigo. O estado fica em um banco próprio no mesmo Postgres, sem acesso ao banco de metadados do Airflow.

Os catálogos versionados em `catalog/` são cópias controladas de `endpoints.json` e `municipios.json` do projeto de origem, sem import em runtime. Consulte [`docs/catalog-provenance.md`](docs/catalog-provenance.md).

Para executar os testes automatizados no container de teste (inclui imports das DAGs e testes HTTP mockados, sem chamadas à API):

```bash
docker compose --profile test run --build --rm airflow-tests
```

Para parar os containers sem remover volumes:

```bash
docker compose down
```

Não use `docker compose down -v` em ambiente com estado que precise ser preservado.

## Componentes previstos

- `catalog/`: cópias versionadas de endpoints e municípios.
- `dags/`: piloto manual de uma partição; recorrência, backfill, reparação e publicação permanecem etapas futuras.
- `src/pipeline_tce/ingestion/`: catálogo, planejamento, retry/backoff, limitação de chamadas e gravação atômica do piloto.
- `tests/`: testes automatizados sem chamadas à API real.
- `docs/architecture-plan.md`: decisões e gates de infraestrutura/migração.

## Segurança e operação

- `.env` é local e ignorado pelo Git; `.env.example` contém apenas placeholders.
- Usuário/senha do banco de metadados, Fernet key, secret key e credencial admin são obrigatórios e independentes.
- A imagem de Airflow de exemplo reproduz a referência antiga para facilitar teste local; validar e fixar a versão suportada no ambiente de destino antes de produção.
- Airflow metadata PostgreSQL não é o banco analítico e não armazena os Parquets.
- Definir backup e restauração dos metadados e do storage compartilhado antes de implantação.

## Próximos gates

1. Confirmar servidor, storage compartilhado, recursos, backup e política de rede/TLS.
2. Confirmar versões compatíveis no destino e estratégia de compartilhamento versionado do extrator e dos schemas.
3. Definir contratos, status por partição, critérios de qualidade bloqueantes e semântica de publicação.
4. Implementar e testar em área isolada; comparar com a origem antes de qualquer leitura dupla/cutover.

Não implantar nem migrar dados até confirmação explícita do responsável.

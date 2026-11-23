# Roadmap — de extrator para pipeline de dados

**Status:** proposta em execução incremental. A Fase 1 (estado, armazenamento e
manifesto) está implementada e ligada à DAG de validação `tce_pipeline_particao`.
Nenhuma publicação de produção está autorizada; o piloto continua gravando
apenas no volume isolado `airflow_pilot_data`.

Este documento complementa `docs/architecture-plan.md` (arquitetura provisória,
gates de infraestrutura e cutover) e `docs/airflow-orchestration.md`
(granularidade, pools e decisões de execução).

## Diagnóstico

O projeto hoje é um **extrator resiliente**, não um pipeline:

- a extração pagina, faz retry/backoff, grava Parquet atomicamente e limita
  disco; já produziu o escopo completo de Fortaleza/2024 (241 partições);
- não há estado consultável: "arquivo existe" é lido como "partição concluída";
- não há contrato versionado, validação linha a linha, publicação, transformação
  nem interface de consumo;
- o armazenamento é um volume Docker local, não um backend compartilhado;
- a observabilidade se resume ao log da task.

## Arquitetura alvo

```text
API TCE-CE
   -> descoberta de partições (DAG)        -> tabela de estado (Postgres)
   -> extração bronze (staging imutável)   -> manifesto + hash + schema
   -> validação de contrato/qualidade      -> bloqueia publicação, não sobrescreve
   -> publicação atômica por dataset       -> manifesto é o commit lógico
   -> transformação silver/gold (dbt+DuckDB)
   -> consulta (Postgres + pg_duckdb)      -> consumidores somente leitura
   -> métricas, linhagem, alertas, catálogo
```

## Camadas, ferramentas e função

| Camada | Ferramenta | Função | Observação |
|---|---|---|---|
| Orquestração | Airflow 2.10 (existente) | Agendar descoberta, extração, validação, publicação e manutenção | Não é o datalake |
| Estado | PostgreSQL, schema `ingestion` | Rastrear status, tentativas, linhas, hash, schema e erro por partição | Banco próprio da plataforma, não o metadado do Airflow |
| Storage | S3-compatível (MinIO on-prem) | Bronze/silver/gold, versionamento e ciclo de vida | Interface `StorageBackend` mantém o código agnóstico |
| Contratos | JSON Schema versionado + Pandera | Fonte única de schema e tipos por endpoint | Uma regra, não duas divergentes |
| Qualidade | dbt tests + checks SQL no DuckDB | Nulos, domínios, unicidade, contagem vs. histórico | Leve para 4 CPU / 7,2 GB |
| Transformação | dbt-core + dbt-duckdb | Silver limpo e gold agregado a partir dos Parquets | SQL versionado e testável |
| Consulta | Postgres + pg_duckdb | Views sobre Parquet ou tabelas materializadas | Já é candidato no plano arquitetural |
| Compactação | Job DuckDB no Airflow | Consolidar arquivos pequenos por endpoint/ano | Resolve o custo de 847 mil partições |
| Observabilidade | Prometheus + Grafana (statsd) | Taxa, falhas, duração, disco, linhas por partição | Métrica de pipeline, não só log |
| Linhagem | OpenLineage + Marquez | Rastrear origem -> gold | Fase posterior |
| Catálogo | dbt docs (inicial) | Documentar datasets, colunas e donos | OpenMetadata só com demanda real |
| Segredos | Docker secrets ou SOPS+age | Credenciais fora do `.env` de produção | Requisito de governança |
| CI/CD | GitHub Actions | Testes, import de DAGs, build/push da imagem | Impede DAG quebrada em produção |

## Decisões registradas

### D1 — Estado de ingestão em Postgres, separado do metadado do Airflow

**Contexto:** a conclusão de uma partição era inferida pela existência do
Parquet; isso não distingue publicado, falho, vazio legítimo e desatualizado.
**Decisão:** tabela `ingestion_partitions` com transições explícitas
(`pending -> running -> extracted -> validated -> published`, além de `no_data`
e `failed`), mantida em um banco da plataforma. Partições com artefato
(`extracted`, `validated`, `published`) ou ausência histórica (`no_data`) não
são reextraídas: só voltam a `pending` por pedido explícito de
reprocessamento. **Motivo:** retomada, relatório e reparo passam a ser
consultáveis, o estado sobrevive à troca de storage e uma reexecução não
sobrescreve Parquet válido nem consome a API sem necessidade.
**Alternativas descartadas:** continuar inferindo pelo arquivo (ambíguo);
guardar estado no metadado do Airflow (acopla ciclo de vida do dado ao
orquestrador).

### D2 — Storage plugável com commit por manifesto

**Contexto:** o volume Docker não é compartilhável nem oferece durabilidade de
produção; `os.replace` não existe em S3/MinIO. **Decisão:** interface
`StorageBackend` (`put_file_atomic`, `put_bytes_atomic`, `exists`,
`read_bytes`, `remove`) e manifesto JSON gravado por último com sha256,
tamanho, contagem, colunas, `run_id` e versões. **Motivo:** o mesmo código
serve ao piloto local e ao storage compartilhado; o manifesto é o commit
lógico. **Alternativas descartadas:** exigir filesystem POSIX em produção
(limita backend); considerar a escrita do objeto como commit (não atômica em
S3).

### D3 — Marcador de vazio é estado, não arquivo permanente

**Contexto:** partições históricas sem dados geram Parquets de zero colunas;
eles são cache negativo válido e não podem ser apagados, mas multiplicam
arquivos. **Decisão:** preservar o marcador durante a migração e passar a
representar ausência histórica como `no_data` na tabela de estado, mantendo o
arquivo apenas enquanto a compatibilidade exigir. **Motivo:** evita
requisições repetidas sem inflar o dataset. **Alternativas descartadas:**
apagar marcadores (perde cache negativo); tratar ausência como falha (gera
retry infinito).

### D4 — Contrato e qualidade com bloqueio de publicação

**Contexto:** hoje só se verifica se o Parquet abre. **Decisão:** contrato
versionado por endpoint validado na extração e checks de qualidade antes da
publicação; falha bloqueante mantém a versão publicada anterior. **Motivo:**
uma carga inválida nunca substitui uma válida. **Alternativas descartadas:**
validação apenas em modo relatório (não protege o consumidor); duplicar regras
entre extração e qualidade (divergem).

### D5 — Airflow orquestra; não armazena nem transforma em memória

**Contexto:** 847 mil partições e 4 CPU / 7,2 GB. **Decisão:** lotes com teto,
pools seriais para a API, transformação em DuckDB e publicação por manifesto.
**Motivo:** cabe no hardware e mantém a API protegida. **Alternativas
descartadas:** uma task mapeada por partição (incha o scheduler); Spark
(recursos e complexidade desproporcionais); Kafka (a fonte é batch, sem CDC).

### D6 — Banco de estado dedicado no Postgres existente

**Contexto:** o estado precisa de transações e chaves únicas; o volume Docker não
serve e um novo container consumiria memória escassa (swap já pressionada).
**Decisão:** criar papel e banco `pipeline_state` dedicados na instância Postgres
que já roda, com provisionamento idempotente no `airflow-init` e senha própria
fora do repositório. **Motivo:** isolamento lógico sem custo de mais um processo;
o estado não compartilha tabelas nem credenciais com o metadado do Airflow.
**Alternativas descartadas:** novo container Postgres (mais memória); usar o
banco de metadados do Airflow (mistura ciclo de vida); pg_duckdb já nesta fase
(antecipa risco da camada de consulta).

## Fases e critérios de aceite

| Fase | Entrega | Critério de aceite | Status |
|---|---|---|---|
| 1 | Estado, `StorageBackend`, manifesto | Partição rastreável e republicável sem apagar versão válida | Implementado |
| 2 | Ligação ponta a ponta e storage compartilhado (MinIO) | Falha/retomada sem sobrescrever versão válida em backend S3 | DAG de validação pronta; backend S3 pendente |
| 3 | Contratos versionados e qualidade bloqueante | Schema inválido não publica | Pendente |
| 4 | Descoberta, incremental e reparo | Descoberta só enfileira o que falta | Pendente |
| 5 | dbt silver/gold e compactação | Dataset consultável e documentado | Pendente |
| 6 | Consulta pg_duckdb e credencial somente leitura | Consumidor lê sem acesso administrativo | Pendente |
| 7 | Métricas, alertas e SLA | Falha notifica antes do consumidor | Pendente |
| 8 | CI/CD, segredos e backup | Deploy com rollback e sem intervenção manual | Pendente |

## Fora de escopo agora

Spark, Kafka/streaming, Great Expectations no runtime, DataHub/OpenMetadata e
malha de tasks por partição. Nenhum deles resolve o gargalo atual e todos
aumentam custo operacional no hardware disponível.

## Riscos

- Backend S3 sem `rename` atômico exige commit por manifesto; testar antes de publicar.
- Dois escritores no mesmo dataset causam divergência; manter um único escritor oficial.
- Compactação mal planejada pode quebrar consumidores; versionar o layout.
- Estado e storage podem divergir após falha; reconciliar por hash/contagem.
- Sem backup do banco de estado e do storage, a retomada perde confiabilidade.

## Próximo passo

A DAG `tce_pipeline_particao` valida a Fase 1 de ponta a ponta. O próximo passo
é exercitá-la no servidor com uma partição controlada e, em seguida, criar o
adaptador S3/MinIO para a Fase 2, mantendo o piloto atual intocado.

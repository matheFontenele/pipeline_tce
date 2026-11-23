# Handoff — desligamento do extrator legado e troca da fonte do front

Documento para a automação/equipe responsável pelo extrator antigo
(`automacao_api_tse`) e para quem opera o front (`refactor_tce`).
Data: 2026-11-23. Servidor `controle-interno-H81` (`172.16.0.200`).

## 1. O que muda

- O front (`refactor_tce`) passa a ler **somente** o lakehouse do `pipeline_tce`:
  `/home/controle-interno/pipeline_tce/lakehouse` (montado `:ro` em `/lakehouse`).
- O extrator antigo **não é mais fonte de dados novos**. A árvore
  `~/automacao_api_tse/data` fica congelada como arquivo histórico de auditoria;
  nada é lido dela em produção.
- A atualização diária passa a ser responsabilidade do `pipeline_tce`
  (DAG `tce_atualizacao_diaria`, 02:30 `America/Fortaleza`, janela = mês
  corrente + mês anterior).

## 2. Estado no momento do handoff

| Item | Situação |
|---|---|
| DAG `tce_extracao_mensal` | **pausada** (`is_paused=True`); última run terminou |
| Dados adotados | 828.294 Parquets / ~13,6 GB, com manifesto e estado por partição |
| 2008–2025 | completo (todos os endpoints/municípios planejados) |
| 2026/01–09 | completo |
| 2026/10 | em preenchimento pela janela diária |
| 2026/11 | sem publicação na API ainda (retentado todo dia) |
| 2026/12 | inexistente — o extrator antigo apagava esses marcadores e a API não publica o mês |

## 3. O que a equipe do extrator antigo deve fazer

1. **Não despausar** `tce_extracao_mensal`. Se quiser garantia extra, remova o
   `schedule` da DAG ou desabilite o arquivo no diretório de DAGs — o
   `pipeline_tce` já cobre a recorrência.
2. **Congelar** `~/automacao_api_tse/data`: não apagar, não "reparar", não
   reexecutar backfill. É a referência de comparação da migração.
3. **Não escrever** em `~/pipeline_tce/lakehouse` — nem por script, nem por
   container/serviço que monte esse caminho. Leitura é liberada; escrita é
   exclusiva do Airflow do `pipeline_tce` (uid 50000).
4. Se o banco `tce_parquet_db` continuar de pé só por histórico, troque o mount
   `/home/controle-interno/automacao_api_tse/data:/data` para `:ro`. Hoje ele
   está `rw`, o que é um risco de escrita na árvore congelada.
5. Partição faltando, esquema estranho ou número divergente: **abrir demanda**
   para o responsável do `pipeline_tce`. Não reextrair por conta própria — isso
   recria a divergência entre as duas árvores.

## 4. O que a equipe do front deve conferir

O `.env` de `refactor_tce` já aponta para o lakehouse correto:

```bash
LAKEHOUSE_SOURCE=/home/controle-interno/pipeline_tce/lakehouse
```

Para o mount valer, recrie o serviço (`docker compose -f
~/refactor_tce/docker-compose.yml up -d api`) e confirme que o container vê
`/lakehouse/bronze/...`. O módulo `src/api/lakehouse.py` já implementa o
contrato (manifesto é o commit, validação de `content_sha256`/`row_count`,
ignora `_`/`.` e `*.tmp`, nunca escreve).

`PARQUET_SOURCE=/home/controle-interno/refactor_tce/data/parquet` é o modo
legado/seed: pode ser removido depois que a busca e o assistente estiverem
100% servidos pelo lakehouse.

Contrato completo de leitura: [`consumer-access.md`](consumer-access.md).

## 5. Regras que não mudaram

- Parquet **com** manifesto é dado válido; `row_count = 0` significa
  "sem dados" (marcador de zero colunas), não corrupção.
- Parquet **sem** manifesto está em escrita: ignorar e tentar depois.
- Nunca deduzir atualidade por `mtime`; usar `content_sha256` + `created_at`.
- O app não acessa o Postgres `pipeline_state`, o banco do Airflow nem o volume
  Docker antigo `pipeline-tce_airflow_pipeline_data` (backup da migração).

## 6. Pendências conhecidas

- **Validação contratual e publicação (Fase 2) não estão liberadas**: os dados
  são `extracted` (bronze conferido contra manifesto), sem gate de qualidade e
  sem pasta `published/`. Consumo é para desenvolvimento/operação interna.
- Credencial read-only para `pipeline_state` ainda não existe; se o front
  precisar de status por partição, é preciso provisionar usuário dedicado — não
  usar credencial administrativa.

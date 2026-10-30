# Desenho operacional do Airflow

**Status:** decisões de desenho para a plataforma nova; carga recorrente/publicação ainda bloqueadas por contratos e infraestrutura pendentes.
**Contexto:** as sugestões de decomposição foram avaliadas em relação à DAG antiga e ao estado atual. A plataforma mantém um piloto manual de partição; não há DAG de ingestão recorrente para refatorar.

## Implementado nesta etapa

- Catálogos `endpoints.json` e `municipios.json` copiados com proveniência/hash e validados antes do uso; nenhuma importação do checkout antigo.
- `tce_extracao_piloto` é manual e exige exatamente uma partição por `dag_run.conf`; está vinculada ao pool `tce_api_pool` (um slot por padrão) e `max_active_runs=1`.
- `tce_planejamento_lote` é uma prévia manual do catálogo completo e intervalo de anos para todos os municípios (12 meses por ano se o endpoint for mensal); apenas conta arquivos existentes e candidatas, sem API ou gravações.
- `tce_extracao_lote` extrai um lote limitado (teto de partições, 3 workers, circuit breaker após 15 falhas consecutivas, progresso com taxa/ETA e resumo) reaproveitando as decisões operacionais do extrator anterior, reimplementadas no pacote próprio. Retoma pelo Parquet existente e grava apenas no volume isolado.
- O extrator mede ocupação do filesystem de saída antes de chamadas e gravações. `TCE_PILOT_MAX_DISK_USAGE=0.90` interrompe no limite sem retry do Airflow; falha a task e preserva arquivos finais concluídos.
- O cliente pagina de 1.000 em 1.000, espaça requests por 1,5 s, mantém uma chamada ativa por pool, respeita retry/backoff para HTTP 400/429 e falhas transitórias, e não registra corpo da resposta/query string.
- A extração piloto grava apenas no volume Docker `airflow_pilot_data`, usa escrita atômica, preserva Parquet válido e reconhece marcador zero-colunas. Não valida schema/qualidade e não publica.
- Imagem Airflow própria instala pandas/PyArrow/requests contra o constraints file da versão configurada; imagem de teste separada adiciona pytest. Testes mockados não chamam a API.

Essa configuração só valida um piloto controlado; não é o limite de produção aprovado. Slots, intervalos, timeouts e backoffs devem ser calibrados com observação controlada e recursos medidos antes de aumentar volume.

## Decisões adotadas

### Prévia de lotes

**Contexto:** estimar uma busca em massa sem iniciar chamadas imprevisíveis à API. **Decisão:** a prévia abrange todos os endpoints e municípios em intervalo explícito de anos; endpoints mensais abrangem os 12 meses por ano. O padrão 2008–2026 reproduz o escopo histórico estimado, mas não dispara extração. A DAG `tce_planejamento_lote` apenas conta partições e nomes de arquivos existentes no volume isolado. **Motivo:** torna o custo visível sem adicionar tasks em massa nem causar efeitos externos. **Alternativas descartadas nesta etapa:** varrer/extrair todo o histórico automaticamente ou expandir uma task por partição; ampliam chamadas e pressão no scheduler sem estimativa prévia. Arquivo existente é apenas uma contagem por nome, não atestado de validade. Execução em lote fica para etapa posterior, após revisar a prévia e definir tamanho/limites.

### Execução em lote

**Contexto:** o extrator anterior roda a matriz completa dentro de um processo, com 3 threads, circuit breaker e progresso, comportamento validado em produção. **Decisão:** portar essas decisões para `src/pipeline_tce/ingestion/batch.py` e expô-las na DAG manual `tce_extracao_lote`, sem importar o projeto de origem em runtime. Mantidos: 3 workers padrão, intervalo mínimo compartilhado entre threads, circuit breaker após 15 falhas consecutivas e log de progresso com taxa/ETA. Acrescentados: teto explícito de partições por execução e resumo agregado em log. **Motivo:** preservar o que já funcionava em volume e, ao mesmo tempo, tornar cada run limitada e retomável. **Alternativas descartadas:** uma task mapeada por partição (milhares de tasks no scheduler) e execução sem teto (runs longas e difícil diagnóstico). O teto é intencionalmente baixo por padrão; ampliá-lo exige medir disco, memória e comportamento da API. A retomada continua baseada na existência de Parquet válido, não em estado persistido.

### Escrita incremental de partições

**Contexto:** o run piloto em lote foi encerrado com código `-9` durante uma partição anual extensa; não houve evidência conclusiva de OOM no kernel, mas o cliente acumulava todas as páginas em uma lista antes de gravar. **Decisão:** converter e gravar cada página em um Parquet temporário com `ParquetWriter`, registrando progresso por página, e substituir o destino atomicamente apenas após a conclusão. Diferenças de schema entre páginas falham a partição sem publicar arquivo parcial. **Motivo:** limitar memória ao tamanho de uma página e tornar partições grandes observáveis. **Alternativa descartada:** continuar acumulando todas as linhas e aumentar RAM/workers sem evidência. Marcadores vazios históricos mantêm sua semântica; arquivo publicado válido continua protegido.

### DAGs por finalidade

Não executar o histórico completo como parte da recorrência. O desenho separa:

1. `tce_carga_recorrente`: identifica apenas partições vencidas/pendentes da janela corrente.
2. `tce_backfill`: execução manual e parametrizada por intervalo, endpoints e municípios.
3. `tce_reparacao`: seleciona partições ausentes, inválidas ou explicitamente indicadas.
4. `tce_publicacao`: publica somente lotes/datasets aprovados; independente da extração.

Os nomes acima são alvos de desenho, ainda não DAGs implantadas. A recorrência consultará estado de ingestão persistido, não fará varredura completa do histórico a cada run.

### Fluxo e gates de publicação

Cada fluxo deverá separar descoberta, extração, validação estrutural, qualidade do dataset, publicação e checks finais/notificação. Validação estrutural e qualidade são responsabilidades distintas e ambas bloqueiam a publicação quando falham em critérios definidos como bloqueantes. Avisos não bloqueantes serão identificados no relatório. Falhas parciais não substituem a versão publicada válida.

Marcador sem dados, falha de API, erro de transporte, corrupção de arquivo e erro de contrato terão estados diferentes. Marcadores Parquet de zero colunas que venham a ser preservados na migração representam cache negativo; não são arquivo corrompido e não serão apagados por reparação genérica.

### Granularidade, grupos e volume do scheduler

- Usar TaskGroups para organizar visualmente preparação, domínios, validação e publicação. O catálogo de endpoints precisará declarar domínio; não classificar os 21 endpoints por suposição.
- Dynamic Task Mapping é candidato apenas depois de medir número de partições e limites de configuração do Airflow.
- Não expandir diretamente uma task para cada endpoint × ano × mês × município. A descoberta produzirá lotes limitados; cada lote terá resultado/status durável por partição para que uma repetição retome somente itens pendentes.
- A falha de um lote deve preservar resultados já concluídos de forma idempotente e permitir reparar as partições que falharam. O tamanho do lote e o máximo de tarefas mapeadas permanecem configuração a definir por medição.
- `max_active_runs`/locks serão usados para impedir duas publicações concorrentes do mesmo dataset. Os valores dependerão da estratégia de publicação e dos recursos do destino.

### Pools e retries

Pools separados são previstos para chamadas à API, validação e publicação. Não fixar slots (por exemplo 4 ou 8) antes de medir CPU/RAM e comportamento de rate limit. A API não deve receber concorrência maior que a validada. O pool de publicação terá exclusão efetiva por dataset; não confiar apenas na fila do scheduler.

Retry do Airflow cobre falhas transitórias da task; retry/backoff/circuit breaker HTTP fica no cliente de API, com limites coordenados para evitar multiplicação excessiva de tentativas. HTTP 400/429, timeout, erro permanente, ausência legítima e erro de dados devem produzir status distintos. Timeout deve cobrir o lote com margem para os retries planejados.

### Estado durável de ingestão

Criar tabela de controle da plataforma separada do banco interno de metadados do Airflow. Airflow metadata DB não é catálogo de dados nem ledger de partições; XCom também não é fonte de verdade operacional.

Campos mínimos por partição: dataset, endpoint, ano, mês, município, status, tentativas, início/fim, linhas, arquivo/URI, versão do schema e mensagem/código de erro. Acrescentar hash/tamanho do artefato e `run_id` se isso for útil para detectar conteúdo repetido e auditar publicação.

Estados mínimos a validar no modelo: `pending`, `running`, `succeeded`, `no_data`, `retryable_error`, `permanent_error`, `validation_failed`, `published`. Transições devem ser idempotentes; concorrência precisa de lock/claim atômico e lease/recuperação de partição abandonada. O motor e local de persistência estão pendentes da arquitetura de storage/banco.

### Logs, relatórios e alertas

- INFO: início/fim de etapa, progresso agregado e resumo periódico configurável por DAG/lote.
- DEBUG: detalhe por partição/arquivo; não imprimir bodies da API, credenciais, headers de autenticação nem URLs contendo parâmetros sensíveis.
- WARNING: retries, ausência fora do padrão, validação com aviso e degradação observável.
- ERROR: falha final, quebra de contrato, qualidade bloqueante ou publicação incompleta.
- Resumo por endpoint/período: total, concluídas, sucesso, ausência legítima, retry, falha, linhas, duração e ETA quando estimável.
- Salvar relatório de qualidade e resultado por partição em armazenamento acessível além do log efêmero do scheduler. A política de retenção e destino será definida com o backend.
- Alertas iniciais: DAG/task falha, partição atrasada, falha parcial, duração anormal, falta de espaço e publicação incompleta. Thresholds de taxa de falha, queda de linhas e aumento de ausência de dados exigem baseline e serão definidos após pilotos; não alertar com números arbitrários.
- Callbacks não devem registrar exceções contendo segredos ou dados de resposta da API.

### Visualização e segurança

O Airflow é interface operacional para agendamento, dependências, retries e logs; não será tratado como dashboard executivo. Uma página/Grafana/Metabase pode ser avaliada depois de existir estado confiável, sem adicionar serviço nesta etapa.

A UI atual está em HTTP. Até confirmar proxy/TLS, firewall e rede, o acesso deve permanecer em rede interna/VPN ou por túnel SSH. Não expor `0.0.0.0` diretamente. Usuários, senha admin, Fernet key e secret key são segredos distintos; a configuração do Compose exige valores e não inclui `admin/admin`. Separar credenciais da plataforma, metadados, publicação e consumidores.

## Plano de implementação incremental

1. **Piloto manual:** após atualizar imagem e configurar pool, testar uma única partição na API pública; revisar logs e artefato isolado. Não publicar nem tocar na origem.
2. **Contrato:** confirmar dono/versão do schema e partição, domínio por endpoint, formato de relatório e critérios de qualidade bloqueantes.
3. **Controle:** escolher persistência isolada do Airflow metadata DB e implementar claim/lock, transições e recuperação com testes de concorrência/idempotência.
4. **Planejador:** implementar descoberta pendente/reparação e batching; teste de limites, partições repetidas e volume máximo do scheduler.
5. **Validação/publicação:** aplicar validação em duas camadas e staging/troca segura; teste de que carga inválida não troca versão publicada e de lock de refresh.
6. **Operação:** calibrar pools, callbacks, alertas e relatórios; conectar primeiro consumidor em leitura dupla após os gates anteriores.

Cada etapa que altere dados publicados exige aprovação explícita antes da execução. Até os gates 1–2, manter somente o piloto manual isolado, sem DAGs inativas que sugiram uma ingestão funcional.

## Pendências que bloqueiam execução de ingestão

- Backend e semântica do storage compartilhado, inclusive publicação/rename atômico e backup.
- Persistência da tabela de controle e credenciais separadas.
- Recursos reais do host e concorrência segura com API TCE.
- Versões suportadas de Airflow, Python, PostgreSQL e `pg_duckdb` no destino.
- Estratégia versionada para reutilizar extrator e schemas sem dependência runtime do repositório de origem.
- Definição aprovada de domínios, janelas da carga recorrente, baseline/thresholds de alertas e retenção de logs/relatórios.

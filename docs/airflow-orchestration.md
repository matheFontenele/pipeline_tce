# Desenho operacional do Airflow

**Status:** decisões de desenho para a plataforma nova; ingestão real ainda bloqueada por contratos e infraestrutura pendentes.  
**Contexto:** as sugestões de decomposição foram avaliadas em relação à DAG antiga e ao estado atual da plataforma. A plataforma contém apenas `platform_smoke_test`; não há DAG de ingestão para refatorar.

## Decisões adotadas

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

1. **Contrato:** confirmar dono/versão do schema e partição, domínio por endpoint, formato de relatório e critérios de qualidade bloqueantes.
2. **Controle:** escolher persistência isolada do Airflow metadata DB e implementar claim/lock, transições e recuperação com testes de concorrência/idempotência.
3. **Planejador:** implementar descoberta pendente/reparação e batching; teste de limites, partições repetidas e volume máximo do scheduler.
4. **Ingestão isolada:** integrar extrator compartilhado/versionado e storage de teste; exercitar timeout, HTTP 400/429, retry, falha parcial e marcador sem dados sem tocar nos Parquets de origem.
5. **Validação/publicação:** aplicar validação em duas camadas e staging/troca segura; teste de que carga inválida não troca versão publicada e de lock de refresh.
6. **Operação:** adicionar pools com slots medidos, callbacks, alertas, relatórios e primeiro consumidor em leitura dupla.

Cada etapa que altere dados publicados exige aprovação explícita antes da execução. Até os gates 1–2, manter apenas a smoke test, sem DAGs inativas que sugiram uma ingestão funcional.

## Pendências que bloqueiam execução de ingestão

- Backend e semântica do storage compartilhado, inclusive publicação/rename atômico e backup.
- Persistência da tabela de controle e credenciais separadas.
- Recursos reais do host e concorrência segura com API TCE.
- Versões suportadas de Airflow, Python, PostgreSQL e `pg_duckdb` no destino.
- Estratégia versionada para reutilizar extrator e schemas sem dependência runtime do repositório de origem.
- Definição aprovada de domínios, janelas da carga recorrente, baseline/thresholds de alertas e retenção de logs/relatórios.

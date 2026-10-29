# Plano arquitetural — Plataforma de dados TCE-CE

**Status:** proposta provisória; não autoriza implantação de produção nem migração.  
**Escopo deste documento:** registrar decisões confirmadas, questões de infraestrutura e uma sequência segura de trabalho. O Compose de desenvolvimento pode ser iniciado com configuração obrigatória e sem conexão aos dados existentes.

## Objetivo e princípios

Centralizar a ingestão oficial dos dados abertos do TCE-CE em uma plataforma independente dos consumidores. O Airflow agenda e monitora; não é o armazenamento dos dados. Aplicações não executam extração e não escrevem nos datasets. Falha de consumidor não interrompe ingestão.

- Parquet continua sendo o formato inicial candidato para os dados publicados, em armazenamento compartilhado configurável, independente dos containers e do checkout da aplicação.
- O acesso do consumidor será por contrato documentado e credenciais próprias de leitura; não haverá acesso administrativo nem dependência de caminhos locais.
- Extração, validação, qualidade, armazenamento e publicação serão componentes separáveis e testáveis. Nenhum módulo do projeto de origem será importado em runtime nem montado como dependência permanente.
- A publicação precisa ser idempotente e controlada: falha de extração/validação não pode substituir uma versão válida. Marcadores Parquet de zero colunas, quando identificados na origem, são estado de cache negativo e não podem ser apagados durante eventual migração.
- CPU, RAM, disco, memória do DuckDB, concorrência e lotes não serão dimensionados até o inventário do destino.

## Decisões provisórias e pendências

| Tema | Diretriz atual | Pendente antes de fixar/implantar |
|---|---|---|
| Git | Remoto `https://github.com/matheFontenele/pipeline_tce.git`; clone feito sem sobrescrever histórico. | Repositório remoto está vazio, sem commits ou arquivos de governança. A branch local `main` ainda não tem commit; primeiro push será criação inicial, sem force. |
| Hospedagem | Não presumir endereço de servidor. `172.16.0.200:8085` é apenas referência histórica do Airflow existente. | Confirmar servidor/ambiente, acesso e janela de implantação com o responsável. |
| Storage | Projetar uma interface configurável e não acoplar o dataset a diretório local de projeto/container. | Confirmar S3, MinIO, volume de rede ou alternativa; durabilidade, acesso, capacidade e backup. Não escolher backend de produção ainda. |
| Consulta | Postgres + `pg_duckdb` é candidato inicial por compatibilidade com consumidores atuais. | Provar compatibilidade no destino e comparar alternativas. Instância/banco e credenciais próprios da plataforma; consumidores somente leitura. Não migrar consumidores nesta fase. |
| Rede e segurança | Airflow e dados privados, apenas em rede interna ou VPN; sem exposição pública direta. Segredos fora do repositório. | Confirmar VPN, firewall, TLS/proxy, bind e identidades antes do deploy. Sem credenciais padrão. |
| Recursos | Nenhuma capacidade de produção confirmada. A referência histórica de 6,7 GiB diz respeito a uma máquina de desenvolvimento, não ao destino. | Inventariar CPU, RAM, disco, espaço livre, backup e recuperação. Só então fixar concorrência e limites. |
| Refatoração | Contratos documentados e integração desacoplada. | Não procurar nem presumir caminho; aguardar indicação do responsável se inspeção for necessária. |
| Versões | Não fixar versões de Airflow, Python, PostgreSQL ou `pg_duckdb` ainda. | Obter versões suportadas no destino e validar matriz de compatibilidade com dependências do extrator. |
| Dados existentes | Piloto em área isolada; nenhuma movimentação, remoção ou sobrescrita dos Parquets atuais. | Definir leitura dupla/comparação e cutover somente após piloto e autorização. |

### Referências observadas no Airflow antigo (não são aprovação de produção)

- `src/orchestration/airflow/Dockerfile` usa `apache/airflow:2.10.4-python3.10`; os requisitos específicos incluem dependências da aplicação Streamlit, contrariando o desacoplamento desejado.
- O Compose antigo usa `LocalExecutor`, PostgreSQL 16 para metadados, monta o repositório inteiro em `/app` e expõe a UI pela porta `8085` sem bind local explícito.
- O Compose contém credenciais padrão `airflow/airflow` e criação tolerante de `admin/admin`. Esses defaults não devem ser reproduzidos.
- A DAG principal faz backfill diário desde 2008, importa worker/validadores do repositório original e executa validação em modo de relatório antes do refresh. Não será copiada como pipeline independente.
- Essas informações servem apenas como linha de base para compatibilidade; confirmar versões e segurança no destino antes de produção.

## Desenho conceitual proposto

```text
API aberta TCE-CE
    -> DAGs Airflow independentes
       -> descoberta de partições / fila de trabalho persistida
       -> extração resiliente e limitada por pool
       -> validação de linha e qualidade do dataset
       -> escrita em staging no storage compartilhado
       -> validação final e publicação atômica/idempotente
       -> relatório, estado e métricas
    -> camada de consulta (Postgres + pg_duckdb como opção a validar)
       -> usuários e aplicações com credenciais somente leitura
```

O desenho detalhado deverá separar carga recorrente, backfill parametrizado, reparação de partições e publicação/refresh. A recorrência deve descobrir apenas partições pendentes, inválidas ou explicitamente solicitadas. Evitar milhares de tasks mapeadas sem medir impacto; avaliar lotes/tabela de trabalho e pools. Não definir scheduler, executor ou número de workers sem os recursos reais.

O estado de ingestão deverá permitir rastrear dataset, endpoint, ano, mês, município, status, tentativas, horários, quantidade de linhas, referência do arquivo, versão do schema e erro. Relatórios de qualidade persistidos distinguirão bloqueios de publicação de avisos e diferenciarão falha de API, erro de dados e ausência legítima de registros.

## Reuso do extrator e contratos

O código atual confirma paginação, retry/backoff para HTTP 400/429 e falhas de conexão, retomada baseada em Parquet existente, escrita atômica e marcadores vazios intencionais. A DAG existente, porém, importa código e caminhos do projeto original; sua validação está em modo de relatório e não bloqueia o refresh. Portanto, não copiar seu comportamento operacional sem revisão.

Antes de selecionar uma estratégia de compartilhamento, mapear dependências e separar o núcleo de ingestão das integrações de UI/Airflow. Opções a avaliar: pacote Python versionado (reuso e releases explícitos, com custo de empacotamento/manutenção); cópia controlada para a plataforma (autonomia e duplicação que exige sincronização); serviço remoto (isolamento maior e custo operacional/rede). Não escolher nem integrar até definir testes de compatibilidade e ownership dos schemas.

Os schemas do projeto de origem são referência para entender contratos, mas a plataforma precisa ter contratos versionados e uma estratégia explícita de atualização. Não manter regras de validação duplicadas e divergentes entre validação linha a linha e qualidade.

## Etapas, gates e rollback

1. **Preparação:** confirmar branch/fluxo Git e inventariar infraestrutura, storage, segurança, backup e versões. Registrar decisões e critérios de aceite. Nenhum deploy.
2. **Especificação e desenho:** definir layout/contrato de partições, estados, checks bloqueantes, publicação e estratégia de compartilhamento do extrator. Criar implementação apenas após os gates de infraestrutura e governança aplicáveis.
3. **Infra isolada:** subir ambiente de teste sem desligar nem modificar o atual. Armazenamento e credenciais separados. Sem dados de produção até aprovação.
4. **Piloto:** executar partições controladas em área isolada; exercitar repetição, retomada, timeout, HTTP 400/429, falha parcial, ausência legítima, Parquet corrompido e publicação incompleta.
5. **Comparação:** comparar schemas, contagens e resultados com o caminho atual; confirmar preservação dos marcadores de cache negativo e que carga inválida mantém a publicação anterior.
6. **Leitura dupla:** conectar um consumidor por vez e comparar ambos os caminhos, mantendo o anterior disponível.
7. **Cutover:** somente após aprovação explícita; demais consumidores migram incrementalmente. Desligar componentes antigos é decisão separada e posterior.

Rollback do piloto: parar a execução nova e manter consumidores no caminho atual. Não mover, apagar nem sobrescrever os Parquets existentes. Qualquer ação futura que possa alterar dados publicados exige explicação do impacto e autorização prévia.

## Riscos principais

- Storage ou rede compartilhada indisponível comprometer disponibilidade ou atomicidade; validar semântica de rename/commit e recuperação no backend escolhido.
- Duas plataformas gravando oficialmente o mesmo dataset causarem corrida ou divergência; manter um único escritor oficial e isolamento do piloto.
- Limite da API, HTTP 400/429 e falhas parciais produzirem lacunas; manter retry com limites, retomada e estado consultável por partição.
- Regras de validação ou schemas divergirem durante extração; preservar contrato versionado e impedir publicação em falha bloqueante.
- Recursos desconhecidos causarem OOM, disco cheio ou impacto em serviços vizinhos; inventariar destino e impor limites conservadores antes de carga real.
- A camada de consulta, permissões ou extensão não ser compatível; validar em instância isolada e verificar acesso somente leitura com credenciais de consumidor.

## Próximas informações necessárias

Antes de definir Compose de produção, versões, limites e cutover, confirmar com o responsável: (1) servidor e acesso; (2) storage e backup; (3) CPU/RAM/disco; (4) rede, VPN, firewall e TLS/proxy; (5) versões suportadas; (6) usuário/consumidor piloto e plano de leitura dupla. Até essas confirmações, a arquitetura permanece agnóstica nesses pontos e não há autorização de deploy ou migração.

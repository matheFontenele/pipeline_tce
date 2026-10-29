# pipeline_tce

Plataforma independente para orquestrar a extração, validação e publicação dos dados abertos do TCE-CE. O Airflow agenda e monitora o pipeline; não é o datalake. Os consumidores não executam extração e terão acesso de leitura por uma interface documentada.

## Estado do projeto

Esta é a fundação de desenvolvimento. Ainda não há integração com a API, storage de produção ou banco de consulta. Hospedagem, storage compartilhado, rede/TLS, recursos, versões suportadas e cutover permanecem pendentes de confirmação. **Não use para ingestão de produção nem migre dados.**

Consulte [`docs/architecture-plan.md`](docs/architecture-plan.md) para arquitetura provisória, decisões, riscos e etapas de migração. As referências ao Airflow antigo são históricas e não aprovam a mesma configuração para produção.

## Desenvolvimento local

Requisitos: Docker Engine e Docker Compose v2. Não é necessário Python instalado no host.

```bash
cp .env.example .env
```

Edite `.env` e substitua todos os valores `REPLACE_*` por segredos únicos gerados localmente. Não use os valores de exemplo em produção nem compartilhe o arquivo. Para evitar caracteres especiais não escapados na URL SQLAlchemy, gere a senha do banco como hexadecimal, por exemplo `openssl rand -hex 32`. Gere a Fernet key com a biblioteca da imagem selecionada, por exemplo `docker run --rm "$AIRFLOW_IMAGE" python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'`; gere também uma secret key aleatória. Nunca registre segredos no Git.

```bash
docker compose config --quiet
docker compose up airflow-init
docker compose up -d
```

A interface local fica em `http://127.0.0.1:8085` por padrão. O bind é configurável via `AIRFLOW_WEBSERVER_BIND`; não altere para `0.0.0.0` sem uma configuração aprovada de rede e proxy/TLS. O Compose contém apenas o banco de metadados isolado do Airflow; não monta o repositório de origem nem dados TCE. Não existe DAG de ingestão habilitada nesta fundação.

Para parar os containers sem remover volumes:

```bash
docker compose down
```

Não use `docker compose down -v` em ambiente com estado que precise ser preservado.

## Componentes previstos

- `dags/`: DAGs independentes (recorrência, backfill, reparação e publicação serão separadas).
- `src/`: ingestão, schemas, validação, qualidade, storage e publicação, a implementar após contratos e infraestrutura serem definidos.
- `tests/`: testes automatizados sem dependência de API real.
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

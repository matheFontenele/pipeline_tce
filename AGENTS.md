# Instruções do repositório

## Governança

- Consulte `docs/architecture-plan.md` antes de alterar arquitetura, operação ou contratos.
- Registre decisões relevantes com contexto, motivo, alternativas e opções descartadas.
- Mantenha a implementação independente de `/home/matheus/automacao_api_tse`; não monte esse checkout nem importe seus módulos em runtime.
- Não acesse, mova, apague nem sobrescreva dados do projeto de origem sem autorização explícita.

## Dados e publicação

- Airflow coordena; armazenamento compartilhado independente é a fonte dos datasets.
- Não habilite cargas recorrentes, publicação ou acesso/movimentação dos dados existentes enquanto storage, rede, credenciais, versões e recursos do destino não estiverem confirmados.
- O piloto manual pode consultar a API pública somente para a partição explicitamente informada e grava exclusivamente no volume isolado `airflow_pilot_data`; não usar para produção nem para ler/escrever no projeto de origem.
- Uma carga inválida jamais pode substituir uma versão publicada válida. Marcadores Parquet de zero colunas podem representar cache negativo: preservá-los e documentar sua semântica.
- Validar linha a linha e qualidade de dataset antes da publicação; contratos devem ter uma única fonte de verdade versionada.
- Aplicações consumidoras usam credenciais próprias somente leitura. Credenciais administrativas não são credenciais de consumidor.

## Segurança e testes

- Segredos só em ambiente/secret manager; `.env` não é versionado. Configuração obrigatória deve falhar cedo.
- Sem credenciais padrão, portas públicas por padrão ou segredos em logs.
- Faça builds e testes via Docker Compose; não dependa do Python do host.
- Inclua testes para novos comportamentos e execute os testes disponíveis antes de publicar.
- Não faça deploy nem migração destrutiva sem confirmação explícita do responsável.

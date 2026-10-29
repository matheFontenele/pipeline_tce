# Instruções do repositório

## Governança

- Consulte `docs/architecture-plan.md` antes de alterar arquitetura, operação ou contratos.
- Registre decisões relevantes com contexto, motivo, alternativas e opções descartadas.
- Mantenha a implementação independente de `/home/matheus/automacao_api_tse`; não monte esse checkout nem importe seus módulos em runtime.
- Não acesse, mova, apague nem sobrescreva dados do projeto de origem sem autorização explícita.

## Dados e publicação

- Airflow coordena; armazenamento compartilhado independente é a fonte dos datasets.
- Não habilite extração real, publicação ou acesso aos dados de produção enquanto storage, rede, credenciais, versões e recursos do destino não estiverem confirmados.
- Uma carga inválida jamais pode substituir uma versão publicada válida. Marcadores Parquet de zero colunas podem representar cache negativo: preservá-los e documentar sua semântica.
- Validar linha a linha e qualidade de dataset antes da publicação; contratos devem ter uma única fonte de verdade versionada.
- Aplicações consumidoras usam credenciais próprias somente leitura. Credenciais administrativas não são credenciais de consumidor.

## Segurança e testes

- Segredos só em ambiente/secret manager; `.env` não é versionado. Configuração obrigatória deve falhar cedo.
- Sem credenciais padrão, portas públicas por padrão ou segredos em logs.
- Faça builds e testes via Docker Compose; não dependa do Python do host.
- Inclua testes para novos comportamentos e execute os testes disponíveis antes de publicar.
- Não faça deploy nem migração destrutiva sem confirmação explícita do responsável.

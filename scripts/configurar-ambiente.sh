#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENV_FILE="$ROOT/.env"

if [[ -e "$ENV_FILE" ]]; then
  echo "Arquivo .env já existe; não foi alterado." >&2
  exit 1
fi

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl é necessário para gerar segredos. Instale-o e tente novamente." >&2
  exit 1
fi

cp "$ROOT/.env.example" "$ENV_FILE"
chmod 600 "$ENV_FILE"

admin_password="$(openssl rand -hex 32)"
db_password="$(openssl rand -hex 32)"
fernet_key="$(openssl rand -base64 32 | tr '+/' '-_' | tr -d '\n')"
webserver_secret="$(openssl rand -hex 32)"

sed -i \
  -e "s|^AIRFLOW_ADMIN_PASSWORD=.*|AIRFLOW_ADMIN_PASSWORD=$admin_password|" \
  -e "s|^AIRFLOW_DB_PASSWORD=.*|AIRFLOW_DB_PASSWORD=$db_password|" \
  -e "s|^AIRFLOW_FERNET_KEY=.*|AIRFLOW_FERNET_KEY=$fernet_key|" \
  -e "s|^AIRFLOW_WEBSERVER_SECRET_KEY=.*|AIRFLOW_WEBSERVER_SECRET_KEY=$webserver_secret|" \
  "$ENV_FILE"

echo "Arquivo .env criado com permissão 600 e segredos aleatórios."
echo "Guarde a senha de acesso do Airflow em um local seguro:"
echo "  usuário: admin"
echo "  senha:   $admin_password"
echo
echo "O conteúdo do .env não foi impresso. Não compartilhe nem versione esse arquivo."
echo "Revise AIRFLOW_IMAGE e a configuração de rede antes de iniciar os containers."

import psycopg2.extensions
import pytest

from pipeline_tce.state.config import (
    admin_dsn_from_env,
    state_dsn_from_env,
    state_settings_from_env,
)
from pipeline_tce.state.errors import StateStoreError

PARTS = {
    "TCE_STATE_DB_HOST": "db.interno",
    "TCE_STATE_DB_PORT": "6543",
    "TCE_STATE_DB_NAME": "estado",
    "TCE_STATE_DB_USER": "papel",
    "TCE_STATE_DB_PASSWORD": "senha com espaço:e@ç",
}


def test_monta_dsn_escapando_senha_com_caracteres_especiais():
    dsn = state_dsn_from_env(PARTS)
    parsed = psycopg2.extensions.parse_dsn(dsn)

    assert parsed["host"] == "db.interno"
    assert parsed["port"] == "6543"
    assert parsed["dbname"] == "estado"
    assert parsed["user"] == "papel"
    assert parsed["password"] == PARTS["TCE_STATE_DB_PASSWORD"]


def test_dsn_explicito_tem_precedencia():
    values = {**PARTS, "TCE_STATE_DSN": "postgresql://x:y@host:5432/z"}
    assert state_dsn_from_env(values) == "postgresql://x:y@host:5432/z"


def test_sem_senha_falha_cedo_sem_expor_credenciais():
    values = {key: value for key, value in PARTS.items() if key != "TCE_STATE_DB_PASSWORD"}

    with pytest.raises(StateStoreError, match="TCE_STATE_DB_PASSWORD"):
        state_dsn_from_env(values)
    with pytest.raises(StateStoreError, match="TCE_STATE_DB_PASSWORD"):
        state_settings_from_env(values)


def test_admin_exige_credenciais_proprias():
    with pytest.raises(StateStoreError, match="administrativas"):
        admin_dsn_from_env(PARTS)


def test_admin_usa_banco_de_manutencao_por_padrao():
    values = {
        **PARTS,
        "TCE_STATE_ADMIN_USER": "admin",
        "TCE_STATE_ADMIN_PASSWORD": "outra",
    }
    parsed = psycopg2.extensions.parse_dsn(admin_dsn_from_env(values))

    assert parsed["dbname"] == "postgres"
    assert parsed["user"] == "admin"

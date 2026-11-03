"""Estado versionado da ingestão por partição.

O estado deixa de ser inferido da existência de um arquivo e passa a ser um
registro consultável com transições explícitas. A tabela vive em um banco
Postgres próprio da plataforma, separado dos metadados do Airflow.
"""

from pipeline_tce.state.config import (
    StateSettings,
    admin_dsn_from_env,
    state_dsn_from_env,
    state_settings_from_env,
)
from pipeline_tce.state.errors import StateStoreError, UnknownPartitionError
from pipeline_tce.state.models import (
    ALLOWED_TRANSITIONS,
    InvalidTransitionError,
    PartitionKey,
    PartitionSnapshot,
    PartitionStatus,
)
from pipeline_tce.state.repository import PartitionRepository

__all__ = [
    "ALLOWED_TRANSITIONS",
    "InvalidTransitionError",
    "PartitionKey",
    "PartitionRepository",
    "PartitionSnapshot",
    "PartitionStatus",
    "StateSettings",
    "StateStoreError",
    "UnknownPartitionError",
    "admin_dsn_from_env",
    "state_dsn_from_env",
    "state_settings_from_env",
]

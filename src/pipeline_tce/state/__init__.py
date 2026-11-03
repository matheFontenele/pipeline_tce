"""Estado versionado da ingestão por partição.

O estado deixa de ser inferido da existência de um arquivo e passa a ser um
registro consultável com transições explícitas. A tabela vive em um banco
Postgres próprio da plataforma, separado dos metadados do Airflow.
"""

from pipeline_tce.state.models import (
    ALLOWED_TRANSITIONS,
    InvalidTransitionError,
    PartitionKey,
    PartitionSnapshot,
    PartitionStatus,
)
from pipeline_tce.state.repository import (
    PartitionRepository,
    StateStoreError,
    UnknownPartitionError,
)

__all__ = [
    "ALLOWED_TRANSITIONS",
    "InvalidTransitionError",
    "PartitionKey",
    "PartitionRepository",
    "PartitionSnapshot",
    "PartitionStatus",
    "StateStoreError",
    "UnknownPartitionError",
]

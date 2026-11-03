"""Modelos do estado de ingestão por partição."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pipeline_tce.ingestion.planning import Partition, partition_filename


class PartitionStatus(str, Enum):
    """Estados possíveis de uma partição.

    ``no_data`` é um resultado legítimo (cache negativo histórico), não uma
    falha. ``failed`` é retomável; a próxima execução pode voltar para
    ``pending`` ou ``running``.
    """

    PENDING = "pending"
    RUNNING = "running"
    EXTRACTED = "extracted"
    VALIDATED = "validated"
    PUBLISHED = "published"
    NO_DATA = "no_data"
    FAILED = "failed"


ALLOWED_TRANSITIONS: dict[PartitionStatus, frozenset[PartitionStatus]] = {
    PartitionStatus.PENDING: frozenset(
        {PartitionStatus.RUNNING, PartitionStatus.FAILED}
    ),
    PartitionStatus.RUNNING: frozenset(
        {
            PartitionStatus.EXTRACTED,
            PartitionStatus.NO_DATA,
            PartitionStatus.FAILED,
        }
    ),
    PartitionStatus.EXTRACTED: frozenset(
        {PartitionStatus.VALIDATED, PartitionStatus.FAILED}
    ),
    PartitionStatus.VALIDATED: frozenset(
        {PartitionStatus.PUBLISHED, PartitionStatus.FAILED}
    ),
    PartitionStatus.PUBLISHED: frozenset({PartitionStatus.PENDING}),
    PartitionStatus.NO_DATA: frozenset({PartitionStatus.PENDING}),
    PartitionStatus.FAILED: frozenset(
        {PartitionStatus.PENDING, PartitionStatus.RUNNING}
    ),
}


class InvalidTransitionError(ValueError):
    """Transição de estado não permitida para a partição."""


def ensure_transition(current: PartitionStatus, target: PartitionStatus) -> None:
    """Valida uma transição antes de qualquer escrita no banco."""
    if current is target:
        return
    if target not in ALLOWED_TRANSITIONS[current]:
        raise InvalidTransitionError(
            f"Transição inválida: {current.value} -> {target.value}"
        )


@dataclass(frozen=True)
class PartitionKey:
    """Identidade lógica de uma partição, sem depender do catálogo em memória."""

    endpoint_prefix: str
    year: int
    month: int | None
    municipality_code: str

    def __post_init__(self) -> None:
        if not self.endpoint_prefix or self.endpoint_prefix.strip() != self.endpoint_prefix:
            raise ValueError("endpoint_prefix deve ser não vazio e sem espaços nas pontas")
        if not 2008 <= self.year <= 2100:
            raise ValueError("year deve estar entre 2008 e 2100")
        if self.month is not None and not 1 <= self.month <= 12:
            raise ValueError("month deve ser None (anual) ou estar entre 1 e 12")
        if (
            not self.municipality_code
            or self.municipality_code.strip() != self.municipality_code
        ):
            raise ValueError(
                "municipality_code deve ser não vazio e sem espaços nas pontas"
            )

    @property
    def storage_month(self) -> int:
        """Valor persistido: 0 representa endpoint anual, evitando NULL na chave."""
        return 0 if self.month is None else self.month

    @property
    def filename(self) -> str:
        return partition_filename(
            self.endpoint_prefix, self.year, self.month, self.municipality_code
        )

    @classmethod
    def from_partition(cls, partition: Partition) -> "PartitionKey":
        return cls(
            endpoint_prefix=partition.endpoint.prefix,
            year=partition.year,
            month=partition.month,
            municipality_code=partition.municipality.code,
        )


@dataclass(frozen=True)
class PartitionSnapshot:
    """Leitura imutável de uma linha da tabela de estado."""

    key: PartitionKey
    status: PartitionStatus
    attempts: int
    row_count: int | None
    content_sha256: str | None
    schema_version: str | None
    storage_uri: str | None
    last_run_id: str | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    extracted_at: datetime | None
    published_at: datetime | None

    @property
    def settled(self) -> bool:
        """Estados que não precisam ser reprocessados sem pedido explícito."""
        return self.status in (PartitionStatus.PUBLISHED, PartitionStatus.NO_DATA)

"""Erros do estado de ingestão."""

from __future__ import annotations


class StateStoreError(RuntimeError):
    """Falha ao acessar o banco de estado; não inclui DSN nem credenciais."""


class UnknownPartitionError(StateStoreError):
    """A partição ainda não foi registrada na tabela de estado."""

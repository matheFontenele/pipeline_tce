"""Contrato de armazenamento independente do backend (local, S3/MinIO)."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable


class StorageError(RuntimeError):
    """Falha de armazenamento; mensagens não incluem credenciais."""


@runtime_checkable
class StorageBackend(Protocol):
    """Interface mínima usada pela ingestão e pela publicação.

    Chaves são caminhos POSIX relativos à raiz (``bronze/endpoint/ano/...``).
    O backend é responsável por tornar a escrita atômica: um leitor nunca deve
    observar um arquivo parcialmente escrito.
    """

    def uri(self, key: str) -> str:
        ...

    def exists(self, key: str) -> bool:
        ...

    def put_file_atomic(self, source: Path, key: str) -> str:
        ...

    def put_bytes_atomic(self, data: bytes, key: str, *, fsync: bool = True) -> str:
        """Grava bytes com commit atômico.

        ``fsync`` pode ser desligado apenas em migração offline em lote.
        """
        ...

    def read_bytes(self, key: str) -> bytes:
        ...

    def sha256(self, key: str) -> str:
        """SHA-256 hexadecimal do objeto, lido em streaming.

        Evita carregar o Parquet inteiro em memória e não expõe o caminho
        físico, permitindo conferência igual em filesystem e S3/MinIO.
        """
        ...

    def remove(self, key: str) -> None:
        ...

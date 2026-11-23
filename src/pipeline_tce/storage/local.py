"""Backend de armazenamento em filesystem local.

Serve ao piloto (volume Docker) e aos testes. A semântica de escrita atômica
depende de ``os.replace`` no mesmo filesystem; ao trocar por S3/MinIO, a
atomicidade passa a ser garantida por manifesto, não por rename.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from pathlib import Path, PurePosixPath

from pipeline_tce.storage.base import StorageError


class LocalStorage:
    """Implementa ``StorageBackend`` sobre um diretório raiz."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        self._resolved_root = self.root.resolve()

    def uri(self, key: str) -> str:
        return str(self._resolve(key))

    def _resolve(self, key: str) -> Path:
        if not key or key.startswith("/") or "\\" in key or "\x00" in key:
            raise StorageError("Chave de armazenamento inválida")
        parts = PurePosixPath(key).parts
        if not parts or any(part in ("", ".", "..") for part in parts):
            raise StorageError("Chave de armazenamento inválida")
        destination = (self.root / PurePosixPath(*parts)).resolve()
        if destination != self._resolved_root and self._resolved_root not in destination.parents:
            raise StorageError("Chave de armazenamento fora da raiz configurada")
        return destination

    def exists(self, key: str) -> bool:
        return self._resolve(key).is_file()

    def put_file_atomic(self, source: Path, key: str) -> str:
        source = Path(source)
        if not source.is_file():
            raise StorageError(f"Origem inexistente para armazenamento: {source.name}")
        destination = self._resolve(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
        )
        os.close(descriptor)
        try:
            shutil.copyfile(source, temporary)
            with open(temporary, "rb") as handle:
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return str(destination)

    def put_bytes_atomic(self, data: bytes, key: str) -> str:
        destination = self._resolve(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, destination)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
        return str(destination)

    def read_bytes(self, key: str) -> bytes:
        path = self._resolve(key)
        try:
            return path.read_bytes()
        except FileNotFoundError as exc:
            raise StorageError(f"Chave inexistente: {key}") from exc

    def sha256(self, key: str) -> str:
        path = self._resolve(key)
        digest = hashlib.sha256()
        try:
            with path.open("rb") as handle:
                for chunk in iter(lambda: handle.read(1 << 20), b""):
                    digest.update(chunk)
        except FileNotFoundError as exc:
            raise StorageError(f"Chave inexistente: {key}") from exc
        return digest.hexdigest()

    def remove(self, key: str) -> None:
        self._resolve(key).unlink(missing_ok=True)

    def list_keys(self, prefix: str = "") -> list[str]:
        """Lista chaves relativas sob um prefixo, em ordem determinística."""
        base = self._resolve(prefix) if prefix else self._resolved_root
        if not base.exists():
            return []
        keys = []
        for path in sorted(p for p in base.rglob("*") if p.is_file()):
            keys.append(path.relative_to(self._resolved_root).as_posix())
        return keys

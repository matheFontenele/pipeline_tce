"""Armazenamento da plataforma, independente do backend e do container."""

from pipeline_tce.storage.base import StorageBackend, StorageError
from pipeline_tce.storage.local import LocalStorage
from pipeline_tce.storage.manifest import (
    MANIFEST_FORMAT_VERSION,
    build_manifest,
    file_sha256,
    manifest_key,
    write_manifest,
)

__all__ = [
    "MANIFEST_FORMAT_VERSION",
    "LocalStorage",
    "StorageBackend",
    "StorageError",
    "build_manifest",
    "file_sha256",
    "manifest_key",
    "write_manifest",
]

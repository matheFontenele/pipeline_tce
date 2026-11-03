import json

import pandas as pd
import pytest

from pipeline_tce.state.models import PartitionKey
from pipeline_tce.storage import (
    LocalStorage,
    StorageError,
    build_manifest,
    file_sha256,
    manifest_key,
    write_manifest,
)

KEY = PartitionKey("notas_empenho", 2024, 1, "057")


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(tmp_path / "root")


def test_grava_arquivo_e_substitui_atomicamente(tmp_path, storage):
    source = tmp_path / "origem.bin"
    source.write_bytes(b"primeira versao")

    uri = storage.put_file_atomic(source, "bronze/notas_empenho/2024/arquivo.parquet")
    assert storage.read_bytes("bronze/notas_empenho/2024/arquivo.parquet") == b"primeira versao"

    source.write_bytes(b"segunda versao")
    storage.put_file_atomic(source, "bronze/notas_empenho/2024/arquivo.parquet")

    assert storage.read_bytes("bronze/notas_empenho/2024/arquivo.parquet") == b"segunda versao"
    assert list((tmp_path / "root").rglob("*.tmp")) == []
    assert storage.uri("bronze/notas_empenho/2024/arquivo.parquet") == uri


@pytest.mark.parametrize(
    "key",
    ["", "/absoluto.parquet", "../fora.parquet", "a/../../fora.parquet", "a\\b.parquet"],
)
def test_rejeita_chave_invalida(storage, key):
    with pytest.raises(StorageError):
        storage.put_bytes_atomic(b"x", key)


def test_lista_existe_e_remove(storage):
    storage.put_bytes_atomic(b"a", "bronze/b/2.parquet")
    storage.put_bytes_atomic(b"b", "bronze/a/1.parquet")

    assert storage.exists("bronze/a/1.parquet")
    assert not storage.exists("bronze/a/ausente.parquet")
    assert storage.list_keys("bronze") == [
        "bronze/a/1.parquet",
        "bronze/b/2.parquet",
    ]

    storage.remove("bronze/a/1.parquet")
    storage.remove("bronze/a/1.parquet")
    assert storage.list_keys("bronze") == ["bronze/b/2.parquet"]


def test_manifesto_descreve_parquet_com_dados(tmp_path, storage):
    path = tmp_path / KEY.filename
    pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_parquet(path, index=False)

    manifest = build_manifest(path, key=KEY, run_id="run-1", schema_version="1")

    assert manifest["partition"]["filename"] == KEY.filename
    assert manifest["row_count"] == 2
    assert manifest["columns"] == ["a", "b"]
    assert manifest["content_sha256"] == file_sha256(path)
    assert manifest["size_bytes"] == path.stat().st_size
    assert manifest["run_id"] == "run-1"

    write_manifest(storage, key=KEY, manifest=manifest)
    stored = json.loads(storage.read_bytes(manifest_key(KEY.filename)))
    assert stored == manifest


def test_manifesto_de_marcador_vazio_tem_zero_linhas(tmp_path):
    path = tmp_path / KEY.filename
    pd.DataFrame().to_parquet(path, index=False)

    manifest = build_manifest(path, key=KEY, run_id="run-2", schema_version="1")

    assert manifest["row_count"] == 0
    assert manifest["columns"] == []


def test_manifesto_recusa_particao_divergente(tmp_path, storage):
    path = tmp_path / KEY.filename
    pd.DataFrame({"a": [1]}).to_parquet(path, index=False)
    manifest = build_manifest(path, key=KEY, run_id="run-3", schema_version="1")
    outra = PartitionKey("notas_empenho", 2024, 2, "057")

    with pytest.raises(ValueError, match="não corresponde"):
        write_manifest(storage, key=outra, manifest=manifest)

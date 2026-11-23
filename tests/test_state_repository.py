import os
import uuid

import psycopg2
import psycopg2.extensions
import pytest

from pipeline_tce.state.models import (
    InvalidTransitionError,
    PartitionKey,
    PartitionStatus,
)
from pipeline_tce.state.repository import (
    PartitionRepository,
    StateStoreError,
    UnknownPartitionError,
)

ADMIN_DSN = os.environ.get("TCE_STATE_TEST_DSN", "").strip()
requires_postgres = pytest.mark.skipif(
    not ADMIN_DSN,
    reason="TCE_STATE_TEST_DSN não definido; teste de integração Postgres ignorado",
)

MONTHLY = PartitionKey("notas_empenho", 2024, 1, "057")
ANNUAL = PartitionKey("itens_notas_fiscais", 2024, None, "057")
SHA = "a" * 64


def _database_dsn(base_dsn: str, database: str) -> str:
    parameters = psycopg2.extensions.parse_dsn(base_dsn)
    parameters["dbname"] = database
    return psycopg2.extensions.make_dsn(**parameters)


@pytest.fixture(scope="session")
def state_dsn():
    if not ADMIN_DSN:
        pytest.skip("TCE_STATE_TEST_DSN não definido")
    database = f"pipeline_state_test_{uuid.uuid4().hex[:12]}"
    admin = psycopg2.connect(ADMIN_DSN, connect_timeout=5)
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute(f'CREATE DATABASE "{database}"')
        yield _database_dsn(ADMIN_DSN, database)
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
        admin.close()


@pytest.fixture
def repository(state_dsn):
    store = PartitionRepository(state_dsn)
    store.ensure_schema()
    with psycopg2.connect(state_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE ingestion_partitions")
    return store


@requires_postgres
def test_schema_e_idempotente_e_banco_comeca_vazio(repository):
    repository.ensure_schema()
    repository.ensure_schema()

    assert repository.counts() == {}
    assert repository.get(MONTHLY) is None


@requires_postgres
def test_ciclo_completo_ate_publicado(repository):
    created = repository.ensure(MONTHLY)
    assert created.status is PartitionStatus.PENDING
    assert created.attempts == 0

    again = repository.ensure(MONTHLY)
    assert again.created_at == created.created_at
    assert again.status is PartitionStatus.PENDING

    running = repository.mark_running(MONTHLY, "run-1")
    assert running.status is PartitionStatus.RUNNING
    assert running.attempts == 1
    assert running.last_run_id == "run-1"

    extracted = repository.mark_extracted(
        MONTHLY,
        run_id="run-1",
        row_count=42,
        content_sha256=SHA,
        schema_version="1",
        storage_uri="/data/bronze/x.parquet",
    )
    assert extracted.status is PartitionStatus.EXTRACTED
    assert extracted.row_count == 42
    assert extracted.content_sha256 == SHA
    assert extracted.extracted_at is not None

    validated = repository.mark_validated(
        MONTHLY, run_id="run-1", schema_version="1"
    )
    assert validated.status is PartitionStatus.VALIDATED

    published = repository.mark_published(
        MONTHLY, run_id="run-1", storage_uri="/data/published/x.parquet"
    )
    assert published.status is PartitionStatus.PUBLISHED
    assert published.published_at is not None
    assert published.settled is True
    assert repository.counts() == {"published": 1}


@requires_postgres
def test_estado_anual_usa_mes_zero_e_nao_colide_com_mensal(repository):
    repository.ensure(MONTHLY)
    repository.ensure(ANNUAL)

    assert repository.get(ANNUAL).key.month is None
    assert repository.counts() == {"pending": 2}


@requires_postgres
def test_transicao_invalida_nao_altera_estado(repository):
    repository.ensure(MONTHLY)

    with pytest.raises(InvalidTransitionError):
        repository.mark_extracted(
            MONTHLY,
            run_id="run-1",
            row_count=1,
            content_sha256=SHA,
            schema_version="1",
            storage_uri="/x",
        )

    assert repository.get(MONTHLY).status is PartitionStatus.PENDING


@requires_postgres
def test_particao_nao_registrada_falha(repository):
    with pytest.raises(UnknownPartitionError):
        repository.mark_running(MONTHLY, "run-1")


@requires_postgres
def test_vazio_legitimo_pode_ser_reprocessado(repository):
    repository.ensure(MONTHLY)
    repository.mark_running(MONTHLY, "run-1")
    no_data = repository.mark_no_data(MONTHLY, run_id="run-1")

    assert no_data.status is PartitionStatus.NO_DATA
    assert no_data.row_count == 0
    assert no_data.settled is True

    requested = repository.request_reprocess(MONTHLY)
    assert requested.status is PartitionStatus.PENDING


@requires_postgres
def test_falha_guarda_erro_limitado_e_permite_retomada(repository):
    repository.ensure(MONTHLY)
    repository.mark_running(MONTHLY, "run-1")

    failed = repository.mark_failed(
        MONTHLY, run_id="run-1", error=("falha\n" * 400)
    )
    assert failed.status is PartitionStatus.FAILED
    assert "\n" not in failed.last_error
    assert len(failed.last_error) <= 1000

    retried = repository.mark_running(MONTHLY, "run-2")
    assert retried.status is PartitionStatus.RUNNING
    assert retried.attempts == 2
    assert retried.last_error is None


@requires_postgres
def test_particao_extraida_so_e_reextraida_apos_pedido_explicito(repository):
    repository.ensure(MONTHLY)
    repository.mark_running(MONTHLY, "run-1")
    extracted = repository.mark_extracted(
        MONTHLY,
        run_id="run-1",
        row_count=3,
        content_sha256=SHA,
        schema_version="1",
        storage_uri="/data/bronze/x.parquet",
    )
    assert extracted.artifact_ready is True

    with pytest.raises(InvalidTransitionError):
        repository.mark_running(MONTHLY, "run-2")
    assert repository.get(MONTHLY).status is PartitionStatus.EXTRACTED

    requested = repository.request_reprocess(MONTHLY)
    assert requested.status is PartitionStatus.PENDING
    assert requested.row_count == 3

    rerun = repository.mark_running(MONTHLY, "run-2")
    assert rerun.status is PartitionStatus.RUNNING
    assert rerun.attempts == 2


@requires_postgres
def test_pendentes_respeitam_limite_e_ordem(repository):
    repository.ensure(PartitionKey("b", 2024, 1, "001"))
    repository.ensure(PartitionKey("a", 2024, 2, "001"))
    repository.ensure(PartitionKey("a", 2024, 1, "001"))

    pending = repository.pending(limit=2)
    assert [item.key.endpoint_prefix for item in pending] == ["a", "a"]
    assert [item.key.month for item in pending] == [1, 2]
    assert len(repository.pending(limit=0)) == 0


@requires_postgres
def test_dsn_ausente_falha_cedo():
    with pytest.raises(StateStoreError, match="TCE_STATE_"):
        PartitionRepository.from_env({})


@requires_postgres
def test_dsn_vazio_e_rejeitado():
    with pytest.raises(ValueError):
        PartitionRepository("   ")


def _adoption_record(key, status="extracted", sha=None, rows=2):
    return {
        "endpoint_prefix": key.endpoint_prefix,
        "year": key.year,
        "month": key.month,
        "municipality_code": key.municipality_code,
        "status": status,
        "row_count": rows,
        "content_sha256": sha or "c" * 64,
        "schema_version": "1",
        "storage_uri": "/data/bronze/x.parquet",
        "extracted_at": None,
    }


@requires_postgres
def test_adopt_many_registra_particao_nova_e_pendente(repository):
    pendente = repository.ensure(PartitionKey("mensal", 2024, 2, "001"))

    resultado = repository.adopt_many(
        [
            _adoption_record(PartitionKey("mensal", 2024, 1, "001")),
            _adoption_record(pendente.key, rows=0, status="no_data"),
        ],
        run_id="import__teste",
    )

    assert resultado == {"written": 2, "preserved": 0}
    extraida = repository.get(PartitionKey("mensal", 2024, 1, "001"))
    assert extraida.status is PartitionStatus.EXTRACTED
    assert extraida.last_run_id == "import__teste"
    assert extraida.attempts == 0
    assert repository.get(pendente.key).status is PartitionStatus.NO_DATA


@requires_postgres
def test_adopt_many_preserva_artefato_existente(repository):
    repository.ensure(MONTHLY)
    repository.mark_running(MONTHLY, "run-1")
    original = repository.mark_extracted(
        MONTHLY,
        run_id="run-1",
        row_count=10,
        content_sha256=SHA,
        schema_version="1",
        storage_uri="/data/bronze/original.parquet",
    )

    resultado = repository.adopt_many(
        [_adoption_record(MONTHLY, sha="d" * 64, rows=99)],
        run_id="import__teste",
    )

    assert resultado == {"written": 0, "preserved": 1}
    atual = repository.get(MONTHLY)
    assert atual.content_sha256 == original.content_sha256
    assert atual.row_count == 10
    assert atual.last_run_id == "run-1"


@requires_postgres
def test_adopt_many_recusa_status_invalido(repository):
    record = _adoption_record(MONTHLY, status="published")

    with pytest.raises(ValueError, match="não adotável"):
        repository.adopt_many([record], run_id="import__teste")

    assert repository.get(MONTHLY) is None


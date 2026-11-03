-- Estado de ingestão por partição da plataforma TCE-CE.
-- Aplicado de forma idempotente por pipeline_tce.state.repository.ensure_schema.
-- `month = 0` representa endpoint anual e evita NULL na chave primária.

CREATE TABLE IF NOT EXISTS ingestion_partitions (
    endpoint_prefix   text        NOT NULL,
    year              integer     NOT NULL CHECK (year BETWEEN 2008 AND 2100),
    month             smallint    NOT NULL DEFAULT 0 CHECK (month BETWEEN 0 AND 12),
    municipality_code text        NOT NULL,
    status            text        NOT NULL CHECK (
        status IN ('pending', 'running', 'extracted', 'validated', 'published', 'no_data', 'failed')
    ),
    attempts          integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    row_count         bigint      CHECK (row_count IS NULL OR row_count >= 0),
    content_sha256    text        CHECK (
        content_sha256 IS NULL OR content_sha256 ~ '^[0-9a-f]{64}$'
    ),
    schema_version    text,
    storage_uri       text,
    last_run_id       text,
    last_error        text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    updated_at        timestamptz NOT NULL DEFAULT now(),
    extracted_at      timestamptz,
    published_at      timestamptz,
    PRIMARY KEY (endpoint_prefix, year, municipality_code, month)
);

CREATE INDEX IF NOT EXISTS ingestion_partitions_status_idx
    ON ingestion_partitions (status);

CREATE INDEX IF NOT EXISTS ingestion_partitions_endpoint_year_idx
    ON ingestion_partitions (endpoint_prefix, year);

CREATE INDEX IF NOT EXISTS ingestion_partitions_updated_idx
    ON ingestion_partitions (updated_at);

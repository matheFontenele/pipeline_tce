"""Ciclo completo de uma partição: estado, storage e manifesto."""

from pipeline_tce.pipeline.partition import (
    PipelineError,
    PartitionRunResult,
    run_partition,
    storage_key,
    verify_partition,
)

__all__ = [
    "PipelineError",
    "PartitionRunResult",
    "run_partition",
    "storage_key",
    "verify_partition",
]

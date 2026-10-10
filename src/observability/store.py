from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from src.common.delta_maintenance import TABLE_PROPERTIES

TIMESTAMP = pa.timestamp("us", tz="UTC")

PIPELINE_RUN = "pipeline_run"
PIPELINE_RUN_STEP = "pipeline_run_step"
PIPELINE_METRICS = "pipeline_metrics"
PIPELINE_ALERTS = "pipeline_alerts"

SCHEMAS = {
    PIPELINE_RUN: pa.schema([
        ("run_id", pa.string()),
        ("job", pa.string()),
        ("mode", pa.string()),
        ("status", pa.string()),
        ("started_at", TIMESTAMP),
        ("ended_at", TIMESTAMP),
        ("duration_s", pa.float64()),
        ("git_sha", pa.string()),
        ("error", pa.string()),
    ]),
    PIPELINE_RUN_STEP: pa.schema([
        ("run_id", pa.string()),
        ("step", pa.string()),
        ("layer", pa.string()),
        ("dataset", pa.string()),
        ("source_file", pa.string()),
        ("contract_version", pa.string()),
        ("inputs", pa.list_(pa.string())),
        ("output_table", pa.string()),
        ("version_before", pa.int64()),
        ("version_after", pa.int64()),
        ("rows_in", pa.int64()),
        ("rows_out", pa.int64()),
        ("rows_rejected", pa.int64()),
        ("status", pa.string()),
        ("started_at", TIMESTAMP),
        ("ended_at", TIMESTAMP),
        ("duration_s", pa.float64()),
        ("error", pa.string()),
    ]),
    PIPELINE_METRICS: pa.schema([
        ("run_id", pa.string()),
        ("layer", pa.string()),
        ("table_name", pa.string()),
        ("source_file", pa.string()),
        ("metric", pa.string()),
        ("value", pa.float64()),
        ("reference_date", pa.date32()),
        ("computed_at", TIMESTAMP),
    ]),
    PIPELINE_ALERTS: pa.schema([
        ("run_id", pa.string()),
        ("rule", pa.string()),
        ("layer", pa.string()),
        ("table_name", pa.string()),
        ("source_file", pa.string()),
        ("metric", pa.string()),
        ("value", pa.float64()),
        ("expected", pa.float64()),
        ("severity", pa.string()),
        ("status", pa.string()),
        ("message", pa.string()),
        ("evaluated_at", TIMESTAMP),
    ]),
}


def table_path(base_dir: Path, table_name: str) -> Path:
    return base_dir / table_name


def append(base_dir: Path, table_name: str, rows: list[dict]) -> None:
    if not rows:
        return
    write_deltalake(
        table_path(base_dir, table_name),
        pa.Table.from_pylist(rows, schema=SCHEMAS[table_name]),
        mode="append",
        schema_mode="merge",
        configuration=TABLE_PROPERTIES,
    )


def read(base_dir: Path, table_name: str) -> pa.Table:
    path = table_path(base_dir, table_name)
    if not DeltaTable.is_deltatable(str(path)):
        return SCHEMAS[table_name].empty_table()
    return DeltaTable(str(path)).to_pyarrow_table()

from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import DeltaTable


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def lag_hours(table: DeltaTable, time_column: str) -> float | None:
    dataset = table.to_pyarrow_dataset()
    if time_column not in dataset.schema.names:
        return None
    events = dataset.to_table(columns=[time_column])
    column = _identifier(time_column)
    con = duckdb.connect()
    con.register("events", events)
    lag_seconds = con.sql(
        f"SELECT epoch(now()) - epoch(max(TRY_CAST({column} AS TIMESTAMPTZ))) FROM events"
    ).fetchone()[0]
    return None if lag_seconds is None else lag_seconds / 3600


def table_metrics(
    path: Path, event_column: str | None = None, arrival_column: str | None = None
) -> dict[str, float]:
    table = DeltaTable(str(path))
    files = pa.table(table.get_add_actions(flatten=True))
    metrics = {
        "table_row_count": float(table.to_pyarrow_dataset().count_rows()),
        "table_file_count": float(files.num_rows),
        "table_size_bytes": float(sum(files["size_bytes"].to_pylist())),
    }
    for metric, column in (("freshness_lag_hours", event_column), ("arrival_lag_hours", arrival_column)):
        lag = lag_hours(table, column) if column else None
        if lag is not None:
            metrics[metric] = lag
    return metrics

from deltalake import DeltaTable

from src.common.delta_maintenance import TABLE_PROPERTIES
from src.observability.store import (
    PIPELINE_METRICS,
    PIPELINE_RUN,
    SCHEMAS,
    append,
    read,
    table_path,
)

METRIC = {
    "run_id": "r1",
    "layer": "bronze",
    "table_name": "orders",
    "source_file": None,
    "metric": "rows_read",
    "value": 10.0,
    "reference_date": None,
    "computed_at": None,
}


def test_read_missing_table_returns_empty_table_with_schema(tmp_path):
    table = read(tmp_path, PIPELINE_RUN)

    assert table.num_rows == 0
    assert table.schema == SCHEMAS[PIPELINE_RUN]


def test_append_without_rows_creates_nothing(tmp_path):
    append(tmp_path, PIPELINE_METRICS, [])

    assert not table_path(tmp_path, PIPELINE_METRICS).exists()


def test_append_and_read_round_trip(tmp_path):
    append(tmp_path, PIPELINE_METRICS, [METRIC])
    append(tmp_path, PIPELINE_METRICS, [{**METRIC, "run_id": "r2", "value": 20.0}])

    table = read(tmp_path, PIPELINE_METRICS)
    assert sorted(table.select(["run_id", "value"]).to_pylist(), key=lambda r: r["run_id"]) == [
        {"run_id": "r1", "value": 10.0},
        {"run_id": "r2", "value": 20.0},
    ]


def test_tables_use_pipeline_table_properties(tmp_path):
    append(tmp_path, PIPELINE_METRICS, [METRIC])

    configuration = DeltaTable(str(table_path(tmp_path, PIPELINE_METRICS))).metadata().configuration
    assert {k: configuration.get(k) for k in TABLE_PROPERTIES} == TABLE_PROPERTIES

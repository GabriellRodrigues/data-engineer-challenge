from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from src.observability.metrics import table_metrics


def iso(hours_ago: float) -> str:
    return (datetime.now(UTC) - timedelta(hours=hours_ago)).isoformat()


def test_table_metrics_count_rows_and_files(tmp_path):
    write_deltalake(tmp_path, pa.table({"id": [1, 2]}))
    write_deltalake(tmp_path, pa.table({"id": [3]}), mode="append")

    metrics = table_metrics(tmp_path)

    assert metrics["table_row_count"] == 3
    assert metrics["table_file_count"] == 2
    assert metrics["table_size_bytes"] > 0
    assert "freshness_lag_hours" not in metrics


def test_freshness_uses_latest_event_timestamp(tmp_path):
    write_deltalake(tmp_path, pa.table({"_timestamp": [iso(48), iso(5), "not-a-date"]}))

    metrics = table_metrics(tmp_path, "_timestamp")

    assert metrics["freshness_lag_hours"] == pytest.approx(5, abs=0.1)


def test_freshness_is_skipped_when_column_is_missing(tmp_path):
    write_deltalake(tmp_path, pa.table({"id": [1]}))

    assert "freshness_lag_hours" not in table_metrics(tmp_path, "_timestamp")


def test_arrival_lag_uses_latest_ingestion_timestamp(tmp_path):
    ingested = [datetime.now(UTC) - timedelta(hours=h) for h in (30, 2)]
    write_deltalake(tmp_path, pa.table({"_ingested_at": pa.array(ingested, pa.timestamp("us", tz="UTC"))}))

    metrics = table_metrics(tmp_path, arrival_column="_ingested_at")

    assert metrics["arrival_lag_hours"] == pytest.approx(2, abs=0.1)
    assert "freshness_lag_hours" not in metrics

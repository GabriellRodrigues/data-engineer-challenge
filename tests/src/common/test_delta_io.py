from datetime import date

import pyarrow as pa
import pytest
from deltalake import CommitProperties, DeltaTable
from deltalake.exceptions import DeltaError

from src.common.delta_io import publish, read_at_version
from src.common.delta_maintenance import TABLE_PROPERTIES

PARTITION = ["reference_date"]


def rows(reference_dates: list[str], values: list[int]) -> pa.Table:
    return pa.table({
        "reference_date": pa.array([date.fromisoformat(d) for d in reference_dates], pa.date32()),
        "value": pa.array(values, pa.int64()),
    })


def content(path) -> list[tuple]:
    table = DeltaTable(str(path)).to_pyarrow_table().sort_by("value")
    return list(zip(
        [d.isoformat() for d in table["reference_date"].to_pylist()],
        table["value"].to_pylist(),
    ))


def test_publish_without_predicate_creates_and_replaces_the_table(tmp_path):
    path = tmp_path / "t"
    publish(path, rows(["2025-03-01"], [1]), partition_by=PARTITION)
    publish(path, rows(["2025-03-02"], [2]), partition_by=PARTITION)

    assert content(path) == [("2025-03-02", 2)]


def test_publish_with_predicate_replaces_only_matching_partitions(tmp_path):
    path = tmp_path / "t"
    publish(path, rows(["2025-03-01", "2025-03-02", "2025-03-03"], [1, 2, 3]), partition_by=PARTITION)

    publish(
        path,
        rows(["2025-03-02"], [20]),
        predicate="reference_date >= '2025-03-02' AND reference_date <= '2025-03-02'",
        partition_by=PARTITION,
    )

    assert content(path) == [("2025-03-01", 1), ("2025-03-03", 3), ("2025-03-02", 20)]


def test_publish_empty_data_with_predicate_clears_the_partition(tmp_path):
    path = tmp_path / "t"
    publish(path, rows(["2025-03-01", "2025-03-02"], [1, 2]), partition_by=PARTITION)

    publish(path, rows([], []), predicate="reference_date = '2025-03-02'", partition_by=PARTITION)

    assert content(path) == [("2025-03-01", 1)]


def test_publish_rejects_rows_outside_the_predicate(tmp_path):
    path = tmp_path / "t"
    publish(path, rows(["2025-03-01"], [1]), partition_by=PARTITION)

    with pytest.raises(DeltaError):
        publish(path, rows(["2025-03-05"], [5]), predicate="reference_date = '2025-03-01'", partition_by=PARTITION)
    assert content(path) == [("2025-03-01", 1)]


def test_publish_twice_with_same_data_keeps_the_same_content(tmp_path):
    path = tmp_path / "t"
    data = rows(["2025-03-01", "2025-03-02"], [1, 2])
    publish(path, data, partition_by=PARTITION)
    predicate = "reference_date = '2025-03-02'"
    publish(path, data.slice(1), predicate=predicate, partition_by=PARTITION)
    publish(path, data.slice(1), predicate=predicate, partition_by=PARTITION)

    assert content(path) == [("2025-03-01", 1), ("2025-03-02", 2)]


def test_publish_records_commit_metadata_and_table_properties(tmp_path):
    path = tmp_path / "t"
    publish(path, rows(["2025-03-01"], [1]), commit_properties=CommitProperties(custom_metadata={"run_id": "r1"}))

    table = DeltaTable(str(path))
    assert table.history(1)[0]["run_id"] == "r1"
    assert TABLE_PROPERTIES.items() <= table.metadata().configuration.items()


def test_publish_without_predicate_accepts_a_new_schema(tmp_path):
    path = tmp_path / "t"
    publish(path, rows(["2025-03-01"], [1]))
    publish(path, pa.table({"other": ["x"]}))

    assert DeltaTable(str(path)).to_pyarrow_table().column_names == ["other"]


def test_read_at_version_returns_the_pinned_snapshot(tmp_path):
    path = tmp_path / "t"
    publish(path, rows(["2025-03-01"], [1]))
    publish(path, rows(["2025-03-01", "2025-03-02"], [1, 2]))

    dataset, version = read_at_version(path, 0)
    latest, latest_version = read_at_version(path)

    assert (version, dataset.count_rows()) == (0, 1)
    assert (latest_version, latest.count_rows()) == (1, 2)

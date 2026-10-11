import pyarrow as pa
import pytest
from deltalake import write_deltalake

from etl_jobs.maintenance import optimize_tables
from src.observability.store import PIPELINE_RUN, PIPELINE_RUN_STEP, read


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"
    observability_dir = tmp_path / "observability"
    monkeypatch.setattr(optimize_tables, "BRONZE_DIR", bronze_dir)
    monkeypatch.setattr(optimize_tables, "OBSERVABILITY_DIR", observability_dir)
    monkeypatch.setattr(optimize_tables, "SILVER_DIR", tmp_path / "silver")
    monkeypatch.setattr(optimize_tables, "BRONZE_ZORDER", {"orders": ["id"], "missing": None})
    monkeypatch.setattr(optimize_tables, "SILVER_ZORDER", {})
    return bronze_dir, observability_dir


def steps(observability_dir) -> dict[str, str]:
    table = read(observability_dir, PIPELINE_RUN_STEP).select(["step", "status"])
    return {row["step"]: row["status"] for row in table.to_pylist()}


def test_each_table_is_a_step(dirs):
    bronze_dir, observability_dir = dirs
    write_deltalake(bronze_dir / "orders", pa.table({"id": [2, 1]}))

    optimize_tables.main()

    recorded = steps(observability_dir)
    assert recorded["optimize:orders"] == "success"
    assert recorded["optimize:missing"] == "skipped"
    assert read(observability_dir, PIPELINE_RUN).select(["job", "status"]).to_pylist() == [
        {"job": "maintenance.optimize", "status": "success"}
    ]


def test_failure_on_one_table_does_not_stop_the_others(dirs, monkeypatch):
    bronze_dir, observability_dir = dirs
    write_deltalake(bronze_dir / "orders", pa.table({"id": [1]}))
    write_deltalake(bronze_dir / "missing", pa.table({"id": [1]}))
    original = optimize_tables.maintain_table

    def flaky(path, zorder_columns=None):
        if path.name == "orders":
            raise RuntimeError("disk full")
        return original(path, zorder_columns)

    monkeypatch.setattr(optimize_tables, "maintain_table", flaky)

    with pytest.raises(SystemExit):
        optimize_tables.main()

    recorded = steps(observability_dir)
    assert recorded["optimize:orders"] == "failed"
    assert recorded["optimize:missing"] == "success"
    assert read(observability_dir, PIPELINE_RUN).select(["status", "error"]).to_pylist() == [
        {"status": "failed", "error": "Maintenance failed for: orders"}
    ]

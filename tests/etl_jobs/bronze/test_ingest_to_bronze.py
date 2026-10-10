from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, write_deltalake

from etl_jobs.bronze import ingest_to_bronze
from src.observability.store import (
    PIPELINE_ALERTS,
    PIPELINE_METRICS,
    PIPELINE_RUN,
    PIPELINE_RUN_STEP,
    read,
)
from src.utils.paths import ALERT_RULES_FILE

CONTRACT = """
apiVersion: v3.0.2
kind: DataContract
id: test.orders
name: orders
version: 1.0.0
status: active
schema:
  - name: orders
    properties:
      - {{name: order_id, logicalType: string, physicalType: varchar, required: true}}
      - name: status
        logicalType: string
        physicalType: varchar
        required: true
        quality:
          - {{type: library, rule: validValues, validValues: [{valid_values}]}}
"""


@pytest.fixture
def layout(tmp_path, monkeypatch):
    source_dir = tmp_path / "source"
    contracts_dir = tmp_path / "contracts"
    bronze_dir = tmp_path / "bronze"
    source_dir.mkdir()
    contracts_dir.mkdir()
    monkeypatch.setattr(ingest_to_bronze, "SOURCE_DATA_DIR", source_dir)
    monkeypatch.setattr(ingest_to_bronze, "BRONZE_CONTRACTS_DIR", contracts_dir)
    monkeypatch.setattr(ingest_to_bronze, "BRONZE_DIR", bronze_dir)
    monkeypatch.setattr(ingest_to_bronze, "OBSERVABILITY_DIR", tmp_path / "observability")
    no_rules = tmp_path / "alert_rules.yml"
    no_rules.write_text("rules: []\n", encoding="utf-8")
    monkeypatch.setattr(ingest_to_bronze, "ALERT_RULES_FILE", no_rules)
    return source_dir, contracts_dir, bronze_dir


def write_contract(contracts_dir, valid_values: str) -> None:
    (contracts_dir / "orders.yml").write_text(CONTRACT.format(valid_values=valid_values), encoding="utf-8")


def write_source(source_dir, table: pa.Table, name: str = "orders_batch_1.parquet") -> None:
    pq.write_table(table, source_dir / name)


def rows(bronze_dir, table: str) -> list[tuple]:
    path = bronze_dir / table
    if not path.exists():
        return []
    return duckdb.sql(
        f"SELECT order_id, status FROM delta_scan('{path.as_posix()}') ORDER BY order_id"
    ).fetchall()


def orders(ids: list[str], statuses: list[str]) -> pa.Table:
    return pa.table({"order_id": ids, "status": statuses})


def test_rerun_skips_already_ingested_file(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1", "2"], ["OK", "OK"]))

    ingest_to_bronze.main([])
    ingest_to_bronze.main([])

    assert rows(bronze_dir, "orders") == [("1", "OK"), ("2", "OK")]


def test_reprocess_recovers_refused_rows_after_contract_fix(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1", "2"], ["OK", "KO"]))
    ingest_to_bronze.main([])
    assert rows(bronze_dir, "orders_refused") == [("2", "KO")]

    write_contract(contracts_dir, "OK, KO")
    ingest_to_bronze.main([])
    assert rows(bronze_dir, "orders") == [("1", "OK")]

    ingest_to_bronze.main(["--reprocess", "orders_batch_1.parquet"])
    assert rows(bronze_dir, "orders") == [("1", "OK"), ("2", "KO")]
    assert rows(bronze_dir, "orders_refused") == []


def test_reprocess_replaces_previous_version_of_resent_file(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1", "2"], ["OK", "OK"]))
    ingest_to_bronze.main([])

    write_source(source_dir, orders(["1", "3"], ["OK", "OK"]))
    ingest_to_bronze.main(["--reprocess", "orders_batch_1.parquet"])

    assert rows(bronze_dir, "orders") == [("1", "OK"), ("3", "OK")]


def test_reprocess_only_touches_selected_files(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1"], ["OK"]), "orders_batch_1.parquet")
    write_source(source_dir, orders(["2"], ["OK"]), "orders_batch_2.parquet")
    ingest_to_bronze.main([])

    ingest_to_bronze.main(["--reprocess", "orders_batch_2.parquet"])

    assert rows(bronze_dir, "orders") == [("1", "OK"), ("2", "OK")]


def test_reprocess_keeps_existing_rows_when_schema_breaks(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1"], ["OK"]))
    ingest_to_bronze.main([])

    write_source(source_dir, pa.table({"order_id": ["1"], "Status": ["OK"]}))
    with pytest.raises(SystemExit) as exit_info:
        ingest_to_bronze.main(["--reprocess", "orders_batch_1.parquet"])

    assert exit_info.value.code == 1
    assert rows(bronze_dir, "orders") == [("1", "OK")]


def test_reprocess_unknown_file_fails(layout):
    with pytest.raises(SystemExit) as exit_info:
        ingest_to_bronze.main(["--reprocess", "missing.parquet"])

    assert exit_info.value.code == 1


@pytest.mark.parametrize(
    ("file_name", "dataset"),
    [
        ("transactions_batch_1.parquet", "transactions"),
        ("transactions_batch_12.parquet", "transactions"),
        ("settlement_2025-03-04.csv", "settlement_paysettler"),
        ("settlement_20250304_02_00.csv", "settlement_paysettler"),
        ("settlement_paysettler.csv", "settlement_paysettler"),
        ("enterprise_company.parquet", "enterprise_company"),
    ],
)
def test_dataset_name(file_name, dataset):
    assert ingest_to_bronze.dataset_name(Path(file_name)) == dataset


def test_csv_is_ingested_as_raw_text(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    (source_dir / "orders.csv").write_text(
        'order_id,status\n007,OK\n"8,1",OK\n9,KO\n', encoding="utf-8"
    )

    ingest_to_bronze.main([])

    assert rows(bronze_dir, "orders") == [("007", "OK"), ("8,1", "OK")]
    assert rows(bronze_dir, "orders_refused") == [("9", "KO")]


def test_csv_with_renamed_header_breaks(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    (source_dir / "orders.csv").write_text("order_id,STATUS\n1,OK\n", encoding="utf-8")

    with pytest.raises(SystemExit) as exit_info:
        ingest_to_bronze.main([])

    assert exit_info.value.code == 1
    assert rows(bronze_dir, "orders") == []


def test_unsupported_files_are_ignored(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1"], ["OK"]), "orders.parquet")
    (source_dir / "notes.txt").write_text("ignore me", encoding="utf-8")

    ingest_to_bronze.main([])

    assert rows(bronze_dir, "orders") == [("1", "OK")]


def ingestion_modes(bronze_dir, table: str) -> list[tuple]:
    path = (bronze_dir / table).as_posix()
    return duckdb.sql(
        f"SELECT order_id, _ingestion_mode FROM delta_scan('{path}') ORDER BY order_id"
    ).fetchall()


def commits(bronze_dir, table: str) -> list[dict]:
    return DeltaTable(str(bronze_dir / table)).history()


def test_rows_are_marked_with_ingestion_mode(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1"], ["OK"]), "orders_batch_1.parquet")
    write_source(source_dir, orders(["2"], ["OK"]), "orders_batch_2.parquet")
    ingest_to_bronze.main([])

    ingest_to_bronze.main(["--reprocess", "orders_batch_2.parquet"])

    assert ingestion_modes(bronze_dir, "orders") == [("1", "ingest"), ("2", "reprocess")]


def test_commits_record_run_and_source_file(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1"], ["OK"]))

    ingest_to_bronze.main([])

    write = commits(bronze_dir, "orders")[0]
    assert write["operation"] == "WRITE"
    assert write["ingestion_mode"] == "ingest"
    assert write["source_file"] == "orders_batch_1.parquet"
    assert len(write["source_file_hash"]) == 64
    assert write["run_id"]
    assert write["contract_version"] == "1.0.0"
    assert "reason" not in write


def test_reprocess_commits_record_reason(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1"], ["OK"]))
    ingest_to_bronze.main([])

    ingest_to_bronze.main(["--reprocess", "orders_batch_1.parquet", "--reason", "contract fixed"])

    write, delete = commits(bronze_dir, "orders")[:2]
    assert (delete["operation"], write["operation"]) == ("DELETE", "WRITE")
    for commit in (delete, write):
        assert commit["ingestion_mode"] == "reprocess"
        assert commit["reason"] == "contract fixed"
    assert delete["run_id"] == write["run_id"]


def test_reason_requires_reprocess(layout):
    with pytest.raises(SystemExit) as exit_info:
        ingest_to_bronze.main(["--reason", "contract fixed"])

    assert exit_info.value.code == 2


def observed(tmp_path, table_name: str, columns: list[str]) -> list[dict]:
    table = read(tmp_path / "observability", table_name)
    return table.select(columns).to_pylist()


def test_run_records_steps_and_file_metrics(layout, tmp_path):
    source_dir, contracts_dir, _bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1", "2"], ["OK", "OK"]))

    ingest_to_bronze.main([])
    ingest_to_bronze.main([])

    runs = observed(tmp_path, PIPELINE_RUN, ["job", "mode", "status"])
    assert runs == [{"job": "bronze.ingest", "mode": "ingest", "status": "success"}] * 2
    steps = observed(
        tmp_path, PIPELINE_RUN_STEP,
        ["step", "status", "contract_version", "rows_in", "rows_out", "rows_rejected"],
    )
    assert sorted(steps, key=lambda s: s["status"]) == [
        {"step": "ingest:orders_batch_1.parquet", "status": "skipped", "contract_version": None,
         "rows_in": None, "rows_out": None, "rows_rejected": None},
        {"step": "ingest:orders_batch_1.parquet", "status": "success", "contract_version": "1.0.0",
         "rows_in": 2, "rows_out": 2, "rows_rejected": 0},
    ]
    file_metrics = {
        m["metric"]: m["value"]
        for m in observed(tmp_path, PIPELINE_METRICS, ["source_file", "metric", "value"])
        if m["source_file"]
    }
    assert file_metrics == {
        "rows_read": 2.0, "rows_accepted": 2.0, "rows_refused": 0.0,
        "refused_rate": 0.0, "undeclared_columns": 0.0,
    }


def test_high_refused_rate_fails_the_run_with_project_rules(layout, tmp_path, monkeypatch):
    source_dir, contracts_dir, bronze_dir = layout
    monkeypatch.setattr(ingest_to_bronze, "ALERT_RULES_FILE", ALERT_RULES_FILE)
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1", "2"], ["OK", "KO"]))

    with pytest.raises(SystemExit) as exit_info:
        ingest_to_bronze.main([])

    assert exit_info.value.code == 1
    assert rows(bronze_dir, "orders") == [("1", "OK")]
    failed = [a for a in observed(tmp_path, PIPELINE_ALERTS, ["rule", "status"]) if a["status"] == "fail"]
    assert failed == [{"rule": "refused_rate_high", "status": "fail"}]
    assert observed(tmp_path, PIPELINE_RUN, ["status", "error"]) == [
        {"status": "failed", "error": "Blocking alerts raised; see pipeline_alerts"}
    ]


def test_schema_break_records_failed_step_and_run(layout, tmp_path):
    source_dir, contracts_dir, _bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, pa.table({"order_id": ["1"], "Status": ["OK"]}))

    with pytest.raises(SystemExit):
        ingest_to_bronze.main([])

    [step] = observed(tmp_path, PIPELINE_RUN_STEP, ["status", "error"])
    assert step["status"] == "failed"
    assert "column 'status' was renamed to 'Status'" in step["error"]
    assert observed(tmp_path, PIPELINE_RUN, ["status", "error"]) == [
        {"status": "failed", "error": "Ingestion failed for: orders_batch_1.parquet"}
    ]


def source_files(bronze_dir, table: str) -> list[tuple]:
    path = (bronze_dir / table).as_posix()
    return duckdb.sql(
        f"SELECT _source_file, count(*) FROM delta_scan('{path}') GROUP BY 1 ORDER BY 1"
    ).fetchall()


def test_files_in_subfolders_are_ingested_with_relative_path(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    (source_dir / "daily").mkdir()
    write_source(source_dir, orders(["1"], ["OK"]), "daily/orders_batch_1.parquet")
    write_source(source_dir, orders(["2"], ["OK"]), "daily/orders_batch_2.parquet")

    ingest_to_bronze.main([])

    assert source_files(bronze_dir, "orders") == [
        ("daily/orders_batch_1.parquet", 1), ("daily/orders_batch_2.parquet", 1)
    ]


def test_identical_copy_in_another_folder_is_skipped(layout, tmp_path):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    (source_dir / "daily").mkdir()
    write_source(source_dir, orders(["1"], ["OK"]), "daily/orders_batch_1.parquet")
    (source_dir / "orders.parquet").write_bytes((source_dir / "daily/orders_batch_1.parquet").read_bytes())

    ingest_to_bronze.main([])

    assert source_files(bronze_dir, "orders") == [("daily/orders_batch_1.parquet", 1)]
    steps = {s["step"]: s["status"] for s in observed(tmp_path, PIPELINE_RUN_STEP, ["step", "status"])}
    assert steps == {"ingest:daily/orders_batch_1.parquet": "success", "ingest:orders.parquet": "skipped"}


def test_reprocess_accepts_relative_path(layout):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    (source_dir / "daily").mkdir()
    write_source(source_dir, orders(["1"], ["OK"]), "daily/orders_batch_1.parquet")
    ingest_to_bronze.main([])

    write_source(source_dir, orders(["9"], ["OK"]), "daily/orders_batch_1.parquet")
    ingest_to_bronze.main(["--reprocess", "daily/orders_batch_1.parquet"])

    assert rows(bronze_dir, "orders") == [("9", "OK")]


def test_ingested_hashes_of_missing_table_is_empty(tmp_path):
    assert ingest_to_bronze.ingested_hashes(tmp_path / "missing") == set()


def test_hash_index_reads_each_table_once(layout, monkeypatch):
    source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    for n in range(3):
        write_source(source_dir, orders([str(n)], ["OK"]), f"orders_batch_{n}.parquet")
    ingest_to_bronze.main([])

    loads = []
    original = ingest_to_bronze.ingested_hashes
    monkeypatch.setattr(ingest_to_bronze, "ingested_hashes", lambda path: loads.append(path) or original(path))

    ingest_to_bronze.main([])

    assert loads == [bronze_dir / "orders"]


def test_hash_index_add_is_visible_without_reloading(tmp_path):
    index = ingest_to_bronze.HashIndex()

    assert not index.contains(tmp_path / "orders", "abc")
    index.add(tmp_path / "orders", "abc")
    assert index.contains(tmp_path / "orders", "abc")


ARRIVAL_RULES = """rules:
  - {name: orders_overdue, type: threshold, metric: arrival_lag_hours, table_name: orders,
     operator: '>', value: 26, severity: error}
"""


def test_overdue_source_fails_even_without_files_in_landing(layout, tmp_path, monkeypatch):
    _source_dir, contracts_dir, bronze_dir = layout
    write_contract(contracts_dir, "OK")
    rules_file = tmp_path / "arrival_rules.yml"
    rules_file.write_text(ARRIVAL_RULES, encoding="utf-8")
    monkeypatch.setattr(ingest_to_bronze, "ALERT_RULES_FILE", rules_file)
    last_arrival = datetime.now(UTC) - timedelta(hours=30)
    write_deltalake(bronze_dir / "orders", pa.table({
        "order_id": ["1"],
        "status": ["OK"],
        "_ingested_at": pa.array([last_arrival], pa.timestamp("us", tz="UTC")),
    }))

    with pytest.raises(SystemExit) as exit_info:
        ingest_to_bronze.main([])

    assert exit_info.value.code == 1
    assert observed(tmp_path, PIPELINE_ALERTS, ["rule", "status"]) == [{"rule": "orders_overdue", "status": "fail"}]


def test_arrival_lag_is_recorded_for_main_tables_only(layout, tmp_path):
    source_dir, contracts_dir, _bronze_dir = layout
    write_contract(contracts_dir, "OK")
    write_source(source_dir, orders(["1", "2"], ["OK", "KO"]))

    ingest_to_bronze.main([])

    lags = {
        m["table_name"]: m["value"]
        for m in observed(tmp_path, PIPELINE_METRICS, ["table_name", "metric", "value"])
        if m["metric"] == "arrival_lag_hours"
    }
    assert set(lags) == {"orders"}
    assert lags["orders"] < 1

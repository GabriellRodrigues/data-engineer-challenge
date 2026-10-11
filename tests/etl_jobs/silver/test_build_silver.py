from datetime import UTC, date, datetime
from decimal import Decimal

import duckdb
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from etl_jobs.silver import build_silver
from src.observability.store import DQ_RESULTS, PIPELINE_RUN, PIPELINE_RUN_STEP, read

SETTLEMENT_FILE = "paysettler/settlement_2025-03-16.csv"
EARLIER_SETTLEMENT_FILE = "paysettler/settlement_2025-03-09.csv"

TRANSACTION_TYPES = {"id": pa.int64(), "amount": pa.float64()}
RUN_TYPES = {"id": pa.int64(), "total_transactions": pa.int64()}
RESULT_TYPES = {
    "id": pa.int64(),
    "run_id": pa.int64(),
    "internal_amount": pa.float64(),
    "processor_amount": pa.float64(),
    "difference": pa.float64(),
}
COMPANY_TYPES = {"id": pa.int64()}


def bronze_table(rows: list[dict], types: dict, source_file: str, ingested_at: datetime) -> pa.Table:
    columns = list(rows[0])
    table = pa.table({
        name: pa.array([row[name] for row in rows], types.get(name, pa.string())) for name in columns
    })
    n = table.num_rows
    return (
        table.append_column("_run_id", pa.array([f"bronze-{ingested_at:%H%M%S}"] * n))
        .append_column("_source_file", pa.array([source_file] * n))
        .append_column("_source_file_hash", pa.array([f"hash-{source_file}-{ingested_at:%H%M%S}"] * n))
        .append_column("_ingestion_mode", pa.array(["ingest"] * n))
        .append_column("_ingested_at", pa.array([ingested_at] * n, pa.timestamp("us", tz="UTC")))
    )


def transaction(transaction_id, amount, created_at, op="I", ts=None, id_=1, status="COMPLETED", merchant="MERCH_001"):
    return {
        "id": id_,
        "transaction_id": transaction_id,
        "merchant_id": merchant,
        "amount": amount,
        "currency": "BRL",
        "status": status,
        "description": "Payment",
        "created_at": created_at,
        "updated_at": ts or created_at,
        "Op": op,
        "_timestamp": ts or created_at,
    }


def settlement(transaction_id, amount, settled_at, reference="PS-1", status="SETTLED", merchant="merch_001"):
    return {
        "transaction_id": transaction_id,
        "merchant_id": merchant,
        "amount": amount,
        "currency": "BRL",
        "settled_at": settled_at,
        "processor_reference": reference,
        "status": status,
    }


def run_event(run_id, reference_date, status, op, ts, total=1):
    return {
        "id": run_id,
        "reference_date": reference_date,
        "file_name": f"settlement_{reference_date}.csv",
        "status": status,
        "total_transactions": total,
        "started_at": f"{reference_date}T02:00:00+00:00",
        "completed_at": None if status == "IN_PROGRESS" else f"{reference_date}T02:30:00+00:00",
        "created_at": f"{reference_date}T02:00:00+00:00",
        "Op": op,
        "_timestamp": ts,
    }


def result(id_, run_id, transaction_id, category, internal, processor, merchant="MERCH_001", ts="2025-03-16T02:10:00+00:00"):
    difference = None if internal is None or processor is None else round(abs(internal - processor), 2)
    return {
        "id": id_,
        "run_id": run_id,
        "transaction_id": transaction_id,
        "merchant_id": merchant,
        "category": category,
        "internal_amount": internal,
        "processor_amount": processor,
        "difference": difference,
        "created_at": ts,
        "Op": "I",
        "_timestamp": ts,
    }


def company(id_, merchant, legal_name, ts, op="I", cnae="4712100"):
    return {
        "id": id_,
        "merchant_id": merchant,
        "legal_name": legal_name,
        "trade_name": "Loja",
        "document": "12.345.678/0001-90",
        "primary_cnae": cnae,
        "created_at": "2024-01-01T00:00:00+00:00",
        "updated_at": ts,
        "Op": op,
        "_timestamp": ts,
    }


LOAD_TIME = datetime(2025, 3, 17, 3, 0, tzinfo=UTC)

DEFAULT_BRONZE = {
    "transactions": (
        [
            transaction("t1", 100.0, "2025-03-15T10:00:00+00:00"),
            transaction("t1", 120.0, "2025-03-15T10:00:00+00:00", "U", "2025-03-15T12:00:00+00:00", 2),
            transaction("t2", 50.0, "2025-03-02T09:00:00+00:00", id_=3),
            transaction("t2", 50.0, "2025-03-02T09:00:00+00:00", id_=4),
            transaction("t3", 70.0, "2025-03-14T08:00:00+00:00", id_=5),
            transaction("t3", 70.0, "2025-03-14T08:00:00+00:00", "D", "2025-03-15T08:00:00+00:00", 6),
        ],
        TRANSACTION_TYPES,
        "transactions_batch_1.parquet",
    ),
    "settlement_paysettler": (
        [
            settlement("t1", "120,00", "2025-03-16T09:00:00", "PS-1"),
            settlement("t4", "R$ 1.152,30", "2025-03-16T10:00:00Z", "PS-4", "REVERSED"),
            settlement("t4", "R$ 1.152,30", "2025-03-16T10:00:00Z", "PS-4", "REVERSED"),
            settlement("t5", "10.5", "2025-03-16T11:00:00+00:00", "PS-5"),
        ],
        {},
        SETTLEMENT_FILE,
    ),
    "reconciliation_runs": (
        [
            run_event(1, "2025-03-16", "IN_PROGRESS", "I", "2025-03-16T02:00:00+00:00", total=3),
            run_event(1, "2025-03-16", "COMPLETED", "U", "2025-03-16T02:30:00+00:00", total=3),
            run_event(2, "2025-03-09", "COMPLETED", "I", "2025-03-09T02:30:00+00:00"),
        ],
        RUN_TYPES,
        "reconciliation_runs.parquet",
    ),
    "reconciliation_results": (
        [
            result(1, 1, "t1", "MATCHED", 120.0, 120.0),
            result(2, 1, "t1", "MATCHED", 120.0, 120.0),
            result(3, 1, "t4", "UNRECONCILED_PROCESSOR", None, 1152.3, merchant=None),
            result(4, 1, "t3", "UNRECONCILED_INTERNAL", 70.0, None),
            result(5, 2, "t2", "MATCHED", 50.0, 50.01, ts="2025-03-09T02:10:00+00:00"),
            result(6, 99, "t2", "MATCHED", 50.0, 50.0),
        ],
        RESULT_TYPES,
        "reconciliation_results.parquet",
    ),
    "enterprise_company": (
        [
            company(1, "MERCH_001", "Empresa Um LTDA", "2024-01-01T00:00:00+00:00"),
            company(1, "MERCH_001", "Empresa Um SA", "2025-01-01T00:00:00+00:00", "U"),
            company(1, "MERCH_001", "Empresa Um SA", "2025-02-01T00:00:00+00:00", "U"),
            company(2, "merch_002", "Empresa Dois LTDA", "2024-06-01T00:00:00+00:00"),
            company(2, "MERCH_002", "Empresa Dois LTDA", "2025-03-01T00:00:00+00:00", "D"),
        ],
        COMPANY_TYPES,
        "enterprise_company.parquet",
    ),
}


@pytest.fixture
def layout(tmp_path, monkeypatch):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    observability_dir = tmp_path / "observability"
    no_rules = tmp_path / "alert_rules.yml"
    no_rules.write_text("rules: []\n", encoding="utf-8")
    monkeypatch.setattr(build_silver, "BRONZE_DIR", bronze_dir)
    monkeypatch.setattr(build_silver, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(build_silver, "OBSERVABILITY_DIR", observability_dir)
    monkeypatch.setattr(build_silver, "ALERT_RULES_FILE", no_rules)
    for name, (rows, types, source_file) in DEFAULT_BRONZE.items():
        write_deltalake(bronze_dir / name, bronze_table(rows, types, source_file, LOAD_TIME), mode="append")
    return bronze_dir, silver_dir, observability_dir


def append_bronze(bronze_dir, name: str, rows: list[dict], source_file: str, ingested_at: datetime) -> None:
    _, types, _ = DEFAULT_BRONZE[name]
    write_deltalake(
        bronze_dir / name, bronze_table(rows, types, source_file, ingested_at), mode="append", schema_mode="merge"
    )


def query(silver_dir, table: str, sql: str) -> list[tuple]:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.register(table, DeltaTable(str(silver_dir / table)).to_pyarrow_dataset())
    return con.sql(sql).fetchall()


def snapshot(silver_dir, table: str) -> list[tuple]:
    columns = [c for c in DeltaTable(str(silver_dir / table)).schema().to_arrow().names if not c.startswith("_silver")]
    selected = ", ".join(f'"{c}"::VARCHAR' for c in columns)
    return query(silver_dir, table, f"SELECT {selected} FROM {table} ORDER BY ALL")


def test_full_build_publishes_every_silver_table(layout):
    _, silver_dir, observability_dir = layout

    build_silver.main([])

    counts = {
        name: DeltaTable(str(silver_dir / name)).to_pyarrow_dataset().count_rows()
        for name in (
            "transactions_history", "transactions", "settlements", "reconciliation_runs",
            "reconciliation_results", "reconciliation_results_quarantine", "merchants",
        )
    }
    assert counts == {
        "transactions_history": 5,
        "transactions": 3,
        "settlements": 3,
        "reconciliation_runs": 2,
        "reconciliation_results": 4,
        "reconciliation_results_quarantine": 1,
        "merchants": 4,
    }
    assert read(observability_dir, PIPELINE_RUN).select(["job", "mode", "status"]).to_pylist() == [
        {"job": "silver.build", "mode": "full", "status": "success"}
    ]
    assert not (silver_dir / "reconciliation_results_deduped").exists()


def test_settlement_amounts_timestamps_and_merchants_are_normalized(layout):
    _, silver_dir, _ = layout
    build_silver.main([])

    assert query(silver_dir, "settlements", """
        SELECT transaction_id, amount, amount_raw, settled_at::VARCHAR, merchant_id, is_reversed, reference_date
        FROM settlements ORDER BY transaction_id
    """) == [
        ("t1", Decimal("120.00"), "120,00", "2025-03-16 09:00:00+00", "MERCH_001", False, date(2025, 3, 16)),
        ("t4", Decimal("1152.30"), "R$ 1.152,30", "2025-03-16 10:00:00+00", "MERCH_001", True, date(2025, 3, 16)),
        ("t5", Decimal("10.50"), "10.5", "2025-03-16 11:00:00+00", "MERCH_001", False, date(2025, 3, 16)),
    ]


def test_settlement_file_without_date_uses_latest_settled_date(layout):
    bronze_dir, silver_dir, _ = layout
    append_bronze(
        bronze_dir,
        "settlement_paysettler",
        [settlement("t9", "1.00", "2025-03-19T23:00:00Z", "PS-9"), settlement("t8", "1.00", "2025-03-20T01:00:00Z", "PS-8")],
        "settlement_paysettler.csv",
        LOAD_TIME,
    )
    build_silver.main([])

    assert query(silver_dir, "settlements", """
        SELECT transaction_id, reference_date, settled_date FROM settlements
        WHERE _source_file = 'settlement_paysettler.csv' ORDER BY transaction_id
    """) == [("t8", date(2025, 3, 20), date(2025, 3, 20)), ("t9", date(2025, 3, 20), date(2025, 3, 19))]


def test_latest_cdc_event_defines_the_current_transaction(layout):
    _, silver_dir, _ = layout
    build_silver.main([])

    assert query(silver_dir, "transactions", """
        SELECT transaction_id, amount, source_id, is_deleted, cdc_event_count, created_date
        FROM transactions ORDER BY transaction_id
    """) == [
        ("t1", Decimal("120.00"), 2, False, 2, date(2025, 3, 15)),
        ("t2", Decimal("50.00"), 4, False, 1, date(2025, 3, 2)),
        ("t3", Decimal("70.00"), 6, True, 2, date(2025, 3, 14)),
    ]


def test_update_wins_over_insert_with_the_same_timestamp(layout):
    bronze_dir, silver_dir, _ = layout
    ts = "2025-03-15T15:00:00+00:00"
    append_bronze(
        bronze_dir,
        "transactions",
        [
            transaction("t7", 10.0, "2025-03-15T15:00:00+00:00", "U", ts, 8, status="FAILED"),
            transaction("t7", 10.0, "2025-03-15T15:00:00+00:00", "I", ts, 9),
        ],
        "transactions_batch_2.parquet",
        LOAD_TIME,
    )
    build_silver.main([])

    assert query(silver_dir, "transactions", "SELECT status FROM transactions WHERE transaction_id = 't7'") == [
        ("FAILED",)
    ]


def test_history_keeps_events_and_collapses_exact_replays(layout):
    _, silver_dir, _ = layout
    build_silver.main([])

    assert query(silver_dir, "transactions_history", """
        SELECT transaction_id, op, source_id FROM transactions_history ORDER BY transaction_id, cdc_timestamp
    """) == [("t1", "I", 1), ("t1", "U", 2), ("t2", "I", 4), ("t3", "I", 5), ("t3", "D", 6)]


def test_missing_optional_bronze_column_becomes_null(layout):
    _, silver_dir, _ = layout
    build_silver.main([])

    assert query(silver_dir, "transactions", "SELECT count(*) FROM transactions WHERE payment_method IS NULL") == [(3,)]


def test_results_are_deduplicated_linked_to_the_run_and_orphans_quarantined(layout):
    _, silver_dir, _ = layout
    build_silver.main([])

    assert query(silver_dir, "reconciliation_results", """
        SELECT run_id, transaction_id, result_id, reference_date, file_name, source_row_count, merchant_id_origin
        FROM reconciliation_results ORDER BY run_id, transaction_id
    """) == [
        (1, "t1", 2, date(2025, 3, 16), "settlement_2025-03-16.csv", 2, "result"),
        (1, "t3", 4, date(2025, 3, 16), "settlement_2025-03-16.csv", 1, "result"),
        (1, "t4", 3, date(2025, 3, 16), "settlement_2025-03-16.csv", 1, "settlement"),
        (2, "t2", 5, date(2025, 3, 9), "settlement_2025-03-09.csv", 1, "result"),
    ]
    assert query(silver_dir, "reconciliation_results_quarantine", """
        SELECT run_id, transaction_id, _quarantine_reason FROM reconciliation_results_quarantine
    """) == [(99, "t2", "run_not_found")]


def test_run_keeps_the_latest_version(layout):
    _, silver_dir, _ = layout
    build_silver.main([])

    assert query(silver_dir, "reconciliation_runs", """
        SELECT run_id, status, duration_s, cdc_event_count FROM reconciliation_runs ORDER BY run_id
    """) == [(1, "COMPLETED", 1800.0, 2), (2, "COMPLETED", 1800.0, 1)]


def test_merchants_are_a_type_2_dimension(layout):
    _, silver_dir, _ = layout
    build_silver.main([])

    assert query(silver_dir, "merchants", """
        SELECT merchant_id, legal_name, valid_from::DATE, valid_to::DATE, is_current, is_deleted, document
        FROM merchants ORDER BY merchant_id, valid_from
    """) == [
        ("MERCH_001", "Empresa Um LTDA", date(2024, 1, 1), date(2025, 1, 1), False, False, "12345678000190"),
        ("MERCH_001", "Empresa Um SA", date(2025, 1, 1), None, True, False, "12345678000190"),
        ("MERCH_002", "Empresa Dois LTDA", date(2024, 6, 1), date(2025, 3, 1), False, False, "12345678000190"),
        ("MERCH_002", "Empresa Dois LTDA", date(2025, 3, 1), None, False, True, "12345678000190"),
    ]


def test_only_the_latest_load_of_a_source_file_is_used(layout):
    bronze_dir, silver_dir, _ = layout
    append_bronze(
        bronze_dir,
        "settlement_paysettler",
        [settlement("t1", "99.99", "2025-03-16T09:00:00Z", "PS-1")],
        SETTLEMENT_FILE,
        datetime(2025, 3, 17, 4, 0, tzinfo=UTC),
    )
    build_silver.main([])

    assert query(silver_dir, "settlements", "SELECT transaction_id, amount FROM settlements") == [
        ("t1", Decimal("99.99"))
    ]


def test_rerun_for_the_same_reference_date_is_idempotent(layout):
    _, silver_dir, _ = layout
    tables = ("transactions", "settlements", "reconciliation_results", "merchants")
    build_silver.main([])
    full = {table: snapshot(silver_dir, table) for table in tables}

    build_silver.main(["--reference-date", "2025-03-16"])
    first = {table: snapshot(silver_dir, table) for table in tables}
    build_silver.main(["--reference-date", "2025-03-16"])
    second = {table: snapshot(silver_dir, table) for table in tables}

    assert full == first == second


def test_reference_date_run_only_replaces_its_partitions(layout):
    bronze_dir, silver_dir, _ = layout
    build_silver.main([])
    append_bronze(
        bronze_dir,
        "settlement_paysettler",
        [settlement("t2", "50.01", "2025-03-09T10:00:00Z", "PS-2")],
        EARLIER_SETTLEMENT_FILE,
        LOAD_TIME,
    )
    append_bronze(
        bronze_dir,
        "settlement_paysettler",
        [settlement("t6", "1.00", "2025-03-16T12:00:00Z", "PS-6")],
        "paysettler/settlement_2025-03-16_resent.csv",
        LOAD_TIME,
    )

    build_silver.main(["--reference-date", "2025-03-16"])
    after_d = query(silver_dir, "settlements", "SELECT transaction_id FROM settlements ORDER BY 1")
    build_silver.main(["--reference-date", "2025-03-09"])
    after_e = query(silver_dir, "settlements", "SELECT transaction_id FROM settlements ORDER BY 1")

    assert after_d == [("t1",), ("t4",), ("t5",), ("t6",)]
    assert after_e == [("t1",), ("t2",), ("t4",), ("t5",), ("t6",)]


def test_transactions_are_replaced_within_the_reconciliation_window(layout):
    bronze_dir, silver_dir, _ = layout
    build_silver.main([])
    append_bronze(
        bronze_dir,
        "transactions",
        [
            transaction("t1", 130.0, "2025-03-15T10:00:00+00:00", "U", "2025-03-16T12:00:00+00:00", 10),
            transaction("t2", 55.0, "2025-03-02T09:00:00+00:00", "U", "2025-03-16T12:00:00+00:00", 11),
        ],
        "transactions_batch_2.parquet",
        LOAD_TIME,
    )

    build_silver.main(["--reference-date", "2025-03-16"])

    assert query(silver_dir, "transactions", """
        SELECT transaction_id, amount FROM transactions WHERE transaction_id IN ('t1', 't2') ORDER BY 1
    """) == [("t1", Decimal("130.00")), ("t2", Decimal("50.00"))]


def test_blocking_check_prevents_publishing(layout, tmp_path, monkeypatch):
    _, silver_dir, observability_dir = layout
    checks = tmp_path / "checks.yml"
    checks.write_text("checks:\n  - {name: always_fails, table: transactions, severity: error, query: SELECT 1}\n")
    monkeypatch.setattr(build_silver, "SILVER_CHECKS_FILE", checks)

    with pytest.raises(SystemExit) as exit_info:
        build_silver.main([])

    assert exit_info.value.code == 1
    assert not silver_dir.exists() or not any(silver_dir.iterdir())
    run = read(observability_dir, PIPELINE_RUN).to_pylist()[0]
    assert run["status"] == "failed"
    assert "always_fails" in run["error"]
    failed = [r["check_name"] for r in read(observability_dir, DQ_RESULTS).to_pylist() if r["status"] == "fail"]
    assert failed == ["always_fails"]


def test_warn_checks_do_not_block_publishing(layout):
    _, silver_dir, observability_dir = layout
    build_silver.main([])

    statuses = {r["check_name"]: r["status"] for r in read(observability_dir, DQ_RESULTS).to_pylist()}
    assert statuses["relationships:reconciliation_results.run_id"] == "warn"
    assert "fail" not in statuses.values()
    assert (silver_dir / "reconciliation_results").exists()


def test_bronze_versions_are_recorded_in_step_inputs(layout):
    _, _, observability_dir = layout
    build_silver.main([])

    steps = {r["step"]: r for r in read(observability_dir, PIPELINE_RUN_STEP).to_pylist()}
    assert steps["compute:reconciliation_results"]["inputs"] == [
        "bronze/reconciliation_results@v0",
        "bronze/reconciliation_runs@v0",
    ]
    assert steps["publish:settlements"]["version_after"] == 0


def test_missing_bronze_table_fails_the_run(layout):
    bronze_dir, _, observability_dir = layout
    for path in (bronze_dir / "enterprise_company").rglob("*"):
        if path.is_file():
            path.unlink()

    with pytest.raises(SystemExit):
        build_silver.main([])

    assert "enterprise_company" in read(observability_dir, PIPELINE_RUN).to_pylist()[0]["error"]


@pytest.mark.parametrize(
    "argv",
    [
        ["--reference-date", "2025-03-16", "--start-date", "2025-03-01"],
        ["--start-date", "2025-03-01"],
        ["--start-date", "2025-03-10", "--end-date", "2025-03-01"],
        ["--reference-date", "16/03/2025"],
    ],
)
def test_invalid_date_arguments_are_rejected(argv):
    with pytest.raises(SystemExit):
        build_silver.parse_args(argv)


def test_date_arguments_resolve_to_a_range():
    single = build_silver.parse_args(["--reference-date", "2025-03-16"])
    window = build_silver.parse_args(["--start-date", "2025-03-01", "--end-date", "2025-03-16"])
    full = build_silver.parse_args([])

    assert (single.start, single.end) == (date(2025, 3, 16), date(2025, 3, 16))
    assert (window.start, window.end) == (date(2025, 3, 1), date(2025, 3, 16))
    assert (full.start, full.end) == (None, None)

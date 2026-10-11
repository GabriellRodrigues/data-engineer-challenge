from datetime import UTC, date, datetime
from decimal import Decimal

import duckdb
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from etl_jobs.gold import build_gold
from etl_jobs.gold.tables import GOLD_TABLES
from src.observability.store import (
    DQ_RESULTS,
    PIPELINE_ALERTS,
    PIPELINE_METRICS,
    PIPELINE_RUN,
    PIPELINE_RUN_STEP,
    read,
)

TS = pa.timestamp("us", tz="UTC")
MONEY = pa.decimal128(18, 2)
LINEAGE = {"_source_file": pa.string(), "_bronze_ingested_at": TS, "_silver_run_id": pa.string()}

SCHEMAS = {
    "transactions": {
        "transaction_id": pa.string(), "merchant_id": pa.string(), "amount": MONEY, "currency": pa.string(),
        "status": pa.string(), "payment_method": pa.string(), "is_deleted": pa.bool_(), "created_at": TS,
        "updated_at": TS, "created_date": pa.date32(), "cdc_event_count": pa.int64(), "last_cdc_timestamp": TS,
    },
    "transactions_history": {
        "transaction_id": pa.string(), "cdc_timestamp": TS, "op": pa.string(), "source_id": pa.int64(),
        "merchant_id": pa.string(), "amount": MONEY, "currency": pa.string(), "status": pa.string(),
        "payment_method": pa.string(), "created_at": TS, "updated_at": TS, "created_date": pa.date32(),
    },
    "settlements": {
        "processor": pa.string(), "reference_date": pa.date32(), "transaction_id": pa.string(),
        "processor_reference": pa.string(), "merchant_id": pa.string(), "amount": MONEY, "currency": pa.string(),
        "status": pa.string(), "is_reversed": pa.bool_(), "settled_at": TS, "settled_date": pa.date32(),
    },
    "reconciliation_runs": {
        "run_id": pa.int64(), "reference_date": pa.date32(), "file_name": pa.string(), "status": pa.string(),
        "total_transactions": pa.int64(), "started_at": TS, "completed_at": TS, "duration_s": pa.float64(),
        "cdc_event_count": pa.int64(),
    },
    "reconciliation_results": {
        "reference_date": pa.date32(), "run_id": pa.int64(), "transaction_id": pa.string(), "result_id": pa.int64(),
        "file_name": pa.string(), "merchant_id": pa.string(), "merchant_id_origin": pa.string(),
        "category": pa.string(), "internal_amount": MONEY, "processor_amount": MONEY, "difference": MONEY,
        "created_at": TS, "source_row_count": pa.int64(),
    },
    "merchants": {
        "merchant_id": pa.string(), "valid_from": TS, "valid_to": TS, "is_current": pa.bool_(),
        "is_deleted": pa.bool_(), "legal_name": pa.string(), "trade_name": pa.string(), "document": pa.string(),
        "primary_cnae": pa.string(),
    },
}


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value).replace(tzinfo=UTC)


def money(value) -> Decimal | None:
    return None if value is None else Decimal(str(value)).quantize(Decimal("0.01"))


def transaction(transaction_id, merchant, amount, currency, status, created, deleted=False, events=1, last=None):
    return {
        "transaction_id": transaction_id, "merchant_id": merchant, "amount": money(amount), "currency": currency,
        "status": status, "payment_method": None, "is_deleted": deleted, "created_at": ts(created),
        "updated_at": ts(last or created), "created_date": ts(created).date(), "cdc_event_count": events,
        "last_cdc_timestamp": ts(last or created),
    }


def event(transaction_id, op, at, merchant, amount, currency, status, created, source_id=1):
    return {
        "transaction_id": transaction_id, "cdc_timestamp": ts(at), "op": op, "source_id": source_id,
        "merchant_id": merchant, "amount": money(amount), "currency": currency, "status": status,
        "payment_method": None, "created_at": ts(created), "updated_at": ts(at), "created_date": ts(created).date(),
    }


def settlement(transaction_id, merchant, amount, currency, status, settled):
    return {
        "processor": "paysettler", "reference_date": date(2025, 3, 16), "transaction_id": transaction_id,
        "processor_reference": f"PS-{transaction_id}", "merchant_id": merchant, "amount": money(amount),
        "currency": currency, "status": status, "is_reversed": status == "REVERSED", "settled_at": ts(settled),
        "settled_date": ts(settled).date(),
    }


def run(run_id, reference_date, status, started, completed=None, total=1):
    started_at, completed_at = ts(started), ts(completed) if completed else None
    return {
        "run_id": run_id, "reference_date": date.fromisoformat(reference_date),
        "file_name": f"settlement_{reference_date}.csv", "status": status, "total_transactions": total,
        "started_at": started_at, "completed_at": completed_at,
        "duration_s": (completed_at - started_at).total_seconds() if completed_at else None, "cdc_event_count": 2,
    }


def result(run_id, reference_date, transaction_id, category, internal, processor, merchant, created, result_id=1):
    difference = None if internal is None or processor is None else abs(money(internal) - money(processor))
    return {
        "reference_date": date.fromisoformat(reference_date), "run_id": run_id, "transaction_id": transaction_id,
        "result_id": result_id, "file_name": f"settlement_{reference_date}.csv", "merchant_id": merchant,
        "merchant_id_origin": "result", "category": category, "internal_amount": money(internal),
        "processor_amount": money(processor), "difference": difference, "created_at": ts(created),
        "source_row_count": 1,
    }


def merchant(merchant_id, legal_name, valid_from, valid_to=None):
    return {
        "merchant_id": merchant_id, "valid_from": ts(valid_from), "valid_to": ts(valid_to) if valid_to else None,
        "is_current": valid_to is None, "is_deleted": False, "legal_name": legal_name, "trade_name": "Loja",
        "document": "12345678000190", "primary_cnae": "4712100",
    }


SILVER = {
    "merchants": [
        merchant("M1", "Um LTDA", "2024-01-01T00:00:00", "2025-03-10T00:00:00"),
        merchant("M1", "Um SA", "2025-03-10T00:00:00"),
        merchant("M2", "Dois LTDA", "2024-01-01T00:00:00"),
    ],
    "transactions": [
        transaction("t1", "M1", 100, "BRL", "COMPLETED", "2025-03-08T10:00:00", events=2, last="2025-03-12T09:00:00"),
        transaction("t2", "M1", 50, "BRL", "COMPLETED", "2025-03-12T10:00:00"),
        transaction("t3", "M2", 70, "USD", "PENDING", "2025-03-14T10:00:00"),
        transaction("t4", "M2", 30, "BRL", "COMPLETED", "2025-03-15T10:00:00", True, 2, "2025-03-16T12:00:00"),
        transaction("t5", "M2", 200, "BRL", "COMPLETED", "2025-03-20T10:00:00"),
    ],
    "transactions_history": [
        event("t1", "I", "2025-03-08T10:00:00", "M1", 100, "BRL", "PENDING", "2025-03-08T10:00:00", 1),
        event("t1", "U", "2025-03-12T09:00:00", "M1", 100, "BRL", "COMPLETED", "2025-03-08T10:00:00", 2),
        event("t2", "I", "2025-03-12T10:00:00", "M1", 50, "BRL", "COMPLETED", "2025-03-12T10:00:00", 3),
        event("t3", "I", "2025-03-14T10:00:00", "M2", 70, "USD", "PENDING", "2025-03-14T10:00:00", 4),
        event("t4", "I", "2025-03-15T10:00:00", "M2", 30, "BRL", "COMPLETED", "2025-03-15T10:00:00", 5),
        event("t4", "D", "2025-03-16T12:00:00", "M2", 30, "BRL", "COMPLETED", "2025-03-15T10:00:00", 6),
        event("t5", "I", "2025-03-20T10:00:00", "M2", 200, "BRL", "COMPLETED", "2025-03-20T10:00:00", 7),
    ],
    "settlements": [
        settlement("t3", "M2", 70, "USD", "SETTLED", "2025-03-16T09:00:00"),
        settlement("t4", "M2", 30, "BRL", "SETTLED", "2025-03-16T09:30:00"),
        settlement("t9", "M2", 10, "BRL", "REVERSED", "2025-03-16T10:00:00"),
    ],
    "reconciliation_runs": [
        run(1, "2025-03-10", "COMPLETED", "2025-03-10T02:00:00", "2025-03-10T02:30:00", total=2),
        run(2, "2025-03-13", "FAILED", "2025-03-13T02:00:00", total=2),
        run(3, "2025-03-13", "COMPLETED", "2025-03-13T05:00:00", "2025-03-13T05:20:00", total=2),
        run(4, "2025-03-16", "COMPLETED", "2025-03-16T02:00:00", "2025-03-16T02:10:00", total=3),
        run(5, "2025-03-22", "COMPLETED", "2025-03-22T02:00:00", "2025-03-22T02:10:00", total=1),
    ],
    "reconciliation_results": [
        result(1, "2025-03-10", "t1", "MISMATCHED", 100, 99.5, "M1", "2025-03-10T02:10:00", 1),
        result(1, "2025-03-10", "p1", "UNRECONCILED_PROCESSOR", None, 40, "M2", "2025-03-10T02:11:00", 2),
        result(3, "2025-03-13", "t1", "MATCHED", 100, 100, "M1", "2025-03-13T05:10:00", 3),
        result(3, "2025-03-13", "t2", "UNRECONCILED_INTERNAL", 50, None, "M1", "2025-03-13T05:11:00", 4),
        result(4, "2025-03-16", "t2", "UNRECONCILED_INTERNAL", 50, None, "M1", "2025-03-16T02:05:00", 5),
        result(4, "2025-03-16", "t3", "MATCHED", 70, 70, "M2", "2025-03-16T02:06:00", 6),
        result(4, "2025-03-16", "t4", "MATCHED", 30, 30, "M2", "2025-03-16T02:07:00", 7),
        result(5, "2025-03-22", "t5", "MATCHED", 200, 200, "M2", "2025-03-22T02:05:00", 8),
    ],
}


def write_silver(silver_dir, name: str, rows: list[dict]) -> None:
    schema = pa.schema({**SCHEMAS[name], **LINEAGE})
    lineage = {"_source_file": f"{name}.parquet", "_bronze_ingested_at": ts("2025-03-23T00:00:00"),
               "_silver_run_id": "silver-1"}
    table = pa.Table.from_pylist([{**row, **lineage} for row in rows], schema=schema)
    write_deltalake(silver_dir / name, table, mode="overwrite")


def prepare(base, monkeypatch):
    silver_dir = base / "silver"
    gold_dir = base / "gold"
    observability_dir = base / "observability"
    rules = base / "alert_rules.yml"
    rules.write_text(
        "rules:\n"
        "  - {name: reconciliation_runs_missing, type: threshold, metric: missing_runs, layer: gold,"
        " operator: '>', value: 0, severity: warn}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(build_gold, "SILVER_DIR", silver_dir)
    monkeypatch.setattr(build_gold, "GOLD_DIR", gold_dir)
    monkeypatch.setattr(build_gold, "OBSERVABILITY_DIR", observability_dir)
    monkeypatch.setattr(build_gold, "ALERT_RULES_FILE", rules)
    for name, rows in SILVER.items():
        write_silver(silver_dir, name, rows)
    return silver_dir, gold_dir, observability_dir


@pytest.fixture
def layout(tmp_path, monkeypatch):
    return prepare(tmp_path, monkeypatch)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    with pytest.MonkeyPatch.context() as monkeypatch:
        dirs = prepare(tmp_path_factory.mktemp("gold_build"), monkeypatch)
        build_gold.main([])
        yield dirs


def query(gold_dir, sql: str) -> list[tuple]:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for table in GOLD_TABLES:
        if (gold_dir / table.name).exists():
            con.register(table.name, DeltaTable(str(gold_dir / table.name)).to_pyarrow_dataset())
    return con.sql(sql).fetchall()


def snapshot(gold_dir, table: str) -> list[tuple]:
    columns = [c for c in DeltaTable(str(gold_dir / table)).schema().to_arrow().names if not c.startswith("_gold")]
    selected = ", ".join(f'"{c}"::VARCHAR' for c in columns)
    return query(gold_dir, f"SELECT {selected} FROM {table} ORDER BY ALL")


def test_full_build_publishes_every_gold_table(built):
    _, gold_dir, observability_dir = built

    counts = {t.name: DeltaTable(str(gold_dir / t.name)).to_pyarrow_dataset().count_rows() for t in GOLD_TABLES}
    assert counts == {
        "dim_date": 15,
        "dim_category": 4,
        "dim_merchant": 4,
        "fact_reconciliation_run": 5,
        "fact_reconciliation_result": 8,
        "fact_transaction_event": 7,
        "fact_transaction": 5,
        "fact_settlement": 3,
        "audit_reconciliation_recheck": 4,
        "agg_reconciliation_daily": 13,
        "agg_amount_at_risk_daily": 6,
        "agg_reconciliation_merchant": 6,
        "agg_volume_daily": 4,
        "agg_merchant_volume_daily": 3,
        "agg_settlement_daily": 2,
    }
    assert read(observability_dir, PIPELINE_RUN).select(["job", "mode", "status"]).to_pylist() == [
        {"job": "gold.build", "mode": "full", "status": "success"}
    ]


def test_merchant_dimension_masks_the_cnpj_and_has_an_unknown_member(built):
    _, gold_dir, _ = built
    rows = query(gold_dir, """
        SELECT merchant_id, legal_name, document_masked, length(document_hash), is_unknown
        FROM dim_merchant ORDER BY merchant_id, valid_from
    """)
    assert rows == [
        ("M1", "Um LTDA", "12.345.678/****-**", 64, False),
        ("M1", "Um SA", "12.345.678/****-**", 64, False),
        ("M2", "Dois LTDA", "12.345.678/****-**", 64, False),
        ("UNKNOWN", "Unknown merchant", None, None, True),
    ]
    assert query(gold_dir, "SELECT count(*) FROM dim_merchant WHERE document_hash = '12345678000190'") == [(0,)]


def test_facts_point_to_the_merchant_version_valid_at_the_transaction(built):
    _, gold_dir, _ = built
    assert query(gold_dir, """
        SELECT t.transaction_id, m.legal_name FROM fact_transaction t
        JOIN dim_merchant m USING (merchant_sk) WHERE t.merchant_id = 'M1' ORDER BY 1
    """) == [("t1", "Um LTDA"), ("t2", "Um SA")]


def test_run_fact_counts_verdicts_and_picks_one_reference_run_per_date(built):
    _, gold_dir, _ = built
    assert query(gold_dir, """
        SELECT run_id, is_reference_run, result_count, discrepancy_count, round(discrepancy_rate, 4), total_gap
        FROM fact_reconciliation_run ORDER BY run_id
    """) == [
        (1, True, 2, 2, 1.0, 0),
        (2, False, 0, 0, None, 2),
        (3, True, 2, 1, 0.5, 0),
        (4, True, 3, 1, 0.3333, 0),
        (5, True, 1, 0, 0.0, 0),
    ]


def test_daily_reconciliation_has_cadence_missing_runs_and_trend(built):
    _, gold_dir, _ = built
    rows = query(gold_dir, """
        SELECT reference_date, cadence_days, is_missing_run, failed_runs, reference_run_id,
               round(discrepancy_rate, 4), round(discrepancy_rate_baseline, 4), round(discrepancy_rate_deviation, 4)
        FROM agg_reconciliation_daily WHERE is_expected_run_date ORDER BY reference_date
    """)
    assert rows == [
        (date(2025, 3, 10), 3, False, 0, 1, 1.0, None, None),
        (date(2025, 3, 13), 3, False, 1, 3, 0.5, 1.0, -0.5),
        (date(2025, 3, 16), 3, False, 0, 4, 0.3333, 0.75, -0.4167),
        (date(2025, 3, 19), 3, True, 0, None, None, None, None),
        (date(2025, 3, 22), 3, False, 0, 5, 0.0, 0.6111, -0.6111),
    ]


def test_amount_at_risk_is_split_by_currency_and_never_mixed(built):
    _, gold_dir, _ = built
    assert query(gold_dir, """
        SELECT reference_date, currency, mismatched_amount, unreconciled_processor_amount,
               unreconciled_internal_amount, amount_at_risk
        FROM agg_amount_at_risk_daily ORDER BY reference_date, currency
    """) == [
        (date(2025, 3, 10), "BRL", Decimal("0.50"), Decimal("0.00"), Decimal("0.00"), Decimal("0.50")),
        (date(2025, 3, 10), "UNKNOWN", Decimal("0.00"), Decimal("40.00"), Decimal("0.00"), Decimal("40.00")),
        (date(2025, 3, 13), "BRL", Decimal("0.00"), Decimal("0.00"), Decimal("50.00"), Decimal("50.00")),
        (date(2025, 3, 16), "BRL", Decimal("0.00"), Decimal("0.00"), Decimal("50.00"), Decimal("50.00")),
        (date(2025, 3, 16), "USD", Decimal("0.00"), Decimal("0.00"), Decimal("0.00"), Decimal("0.00")),
        (date(2025, 3, 22), "BRL", Decimal("0.00"), Decimal("0.00"), Decimal("0.00"), Decimal("0.00")),
    ]


def test_transaction_fact_carries_the_current_reconciliation_status(built):
    _, gold_dir, _ = built
    assert query(gold_dir, """
        SELECT transaction_id, runs_count, reconciliation_status, is_resolved_discrepancy, exposure_amount,
               days_unreconciled, changes_after_reconciliation
        FROM fact_transaction ORDER BY transaction_id
    """) == [
        ("t1", 2, "MATCHED", True, Decimal("0.00"), None, 1),
        ("t2", 2, "UNRECONCILED_INTERNAL", False, Decimal("50.00"), 4, 0),
        ("t3", 1, "MATCHED", False, Decimal("0.00"), None, 0),
        ("t4", 1, "MATCHED", False, Decimal("0.00"), None, 1),
        ("t5", 1, "MATCHED", False, Decimal("0.00"), None, 0),
    ]


def test_transaction_events_record_what_changed(built):
    _, gold_dir, _ = built
    assert query(gold_dir, """
        SELECT transaction_id, event_seq, op, previous_status, status, status_changed, is_after_reconciliation
        FROM fact_transaction_event WHERE transaction_id IN ('t1', 't4') ORDER BY transaction_id, event_seq
    """) == [
        ("t1", 1, "I", None, "PENDING", False, False),
        ("t1", 2, "U", "PENDING", "COMPLETED", True, True),
        ("t4", 1, "I", None, "COMPLETED", False, False),
        ("t4", 2, "D", "COMPLETED", "COMPLETED", False, True),
    ]


def test_volume_excludes_deletes_and_keeps_currencies_apart(built):
    _, gold_dir, _ = built
    assert query(gold_dir, """
        SELECT currency, status, sum(transaction_count), sum(volume_amount)
        FROM agg_volume_daily GROUP BY ALL ORDER BY ALL
    """) == [("BRL", "COMPLETED", 3, Decimal("350.00")), ("USD", "PENDING", 1, Decimal("70.00"))]
    assert query(gold_dir, """
        SELECT currency, settled_amount, reversed_amount, net_amount, without_internal_count
        FROM agg_settlement_daily ORDER BY currency
    """) == [
        ("BRL", Decimal("30.00"), Decimal("10.00"), Decimal("20.00"), 1),
        ("USD", Decimal("70.00"), Decimal("0.00"), Decimal("70.00"), 0),
    ]


def test_recheck_compares_service_verdicts_with_a_recomputation(built):
    _, gold_dir, _ = built
    assert query(gold_dir, """
        SELECT transaction_id, service_category, recomputed_category, outcome
        FROM audit_reconciliation_recheck ORDER BY transaction_id
    """) == [
        ("t2", "UNRECONCILED_INTERNAL", "UNRECONCILED_INTERNAL", "agrees"),
        ("t3", "MATCHED", "MATCHED", "agrees"),
        ("t4", "MATCHED", "UNRECONCILED_PROCESSOR", "category_differs"),
        ("t9", None, "UNRECONCILED_PROCESSOR", "missing_in_service"),
    ]


def test_rerun_for_the_same_reference_date_is_idempotent(layout):
    _, gold_dir, _ = layout
    tables = [t.name for t in GOLD_TABLES]
    build_gold.main([])
    full = {table: snapshot(gold_dir, table) for table in tables}

    build_gold.main(["--reference-date", "2025-03-16"])
    first = {table: snapshot(gold_dir, table) for table in tables}
    build_gold.main(["--reference-date", "2025-03-16"])
    second = {table: snapshot(gold_dir, table) for table in tables}

    assert full == first == second


def test_reference_date_run_only_replaces_its_partitions(layout):
    silver_dir, gold_dir, _ = layout
    build_gold.main([])
    write_silver(silver_dir, "reconciliation_results",
                 [r for r in SILVER["reconciliation_results"] if r["run_id"] != 1])

    build_gold.main(["--reference-date", "2025-03-16"])
    after_other_date = query(gold_dir, "SELECT count(*) FROM fact_reconciliation_result WHERE run_id = 1")
    build_gold.main(["--reference-date", "2025-03-10"])
    after_own_date = query(gold_dir, "SELECT count(*) FROM fact_reconciliation_result WHERE run_id = 1")

    assert after_other_date == [(2,)]
    assert after_own_date == [(0,)]


def test_blocking_check_prevents_publishing(layout, tmp_path, monkeypatch):
    _, gold_dir, observability_dir = layout
    checks = tmp_path / "checks.yml"
    checks.write_text("checks:\n  - {name: always_fails, table: fact_transaction, severity: error, query: SELECT 1}\n")
    monkeypatch.setattr(build_gold, "GOLD_CHECKS_FILE", checks)

    with pytest.raises(SystemExit) as exit_info:
        build_gold.main([])

    assert exit_info.value.code == 1
    assert not any(gold_dir.iterdir())
    assert "always_fails" in read(observability_dir, PIPELINE_RUN).to_pylist()[0]["error"]


def test_shipped_checks_pass_or_only_warn(built):
    _, _, observability_dir = built
    statuses = {r["check_name"]: r["status"] for r in read(observability_dir, DQ_RESULTS).to_pylist()}
    assert "fail" not in statuses.values()
    assert statuses["missing_run:agg_reconciliation_daily"] == "warn"
    assert statuses["reconciliation_recheck:audit_reconciliation_recheck"] == "warn"
    assert statuses["currency_unknown:fact_reconciliation_result"] == "warn"
    assert statuses["sum_conservation:agg_volume_daily"] == "pass"


def test_business_metrics_feed_the_alerts(built):
    _, _, observability_dir = built
    metrics = {
        r["metric"]: (r["value"], r["reference_date"])
        for r in read(observability_dir, PIPELINE_METRICS).to_pylist()
        if r["table_name"] == "agg_reconciliation_daily"
    }
    assert metrics["discrepancy_rate"] == (0.0, date(2025, 3, 22))
    assert metrics["discrepancy_rate_deviation"][0] == pytest.approx(-0.6111, abs=1e-4)
    assert metrics["missing_runs"] == (1.0, None)
    alerts = [r["rule"] for r in read(observability_dir, PIPELINE_ALERTS).to_pylist() if r["status"] == "warn"]
    assert alerts == ["reconciliation_runs_missing"]


def test_scoped_run_reports_only_missing_runs_in_its_scope(layout):
    _, _, observability_dir = layout
    build_gold.main(["--reference-date", "2025-03-16"])

    metrics = {r["metric"]: r["value"] for r in read(observability_dir, PIPELINE_METRICS).to_pylist()}
    assert metrics["missing_runs"] == 0.0


def test_silver_versions_are_recorded_in_step_inputs(built):
    _, _, observability_dir = built
    steps = {r["step"]: r for r in read(observability_dir, PIPELINE_RUN_STEP).to_pylist()}
    assert steps["compute:fact_settlement"]["inputs"] == [
        "silver/settlements@v0", "silver/transactions@v0", "silver/merchants@v0"
    ]
    assert steps["publish:fact_settlement"]["version_after"] == 0


def test_empty_silver_builds_empty_gold(layout):
    silver_dir, gold_dir, _ = layout
    for name in SILVER:
        write_silver(silver_dir, name, [])

    build_gold.main([])

    assert query(gold_dir, "SELECT count(*) FROM fact_reconciliation_result") == [(0,)]
    assert query(gold_dir, "SELECT count(*) FROM agg_reconciliation_daily") == [(0,)]
    assert query(gold_dir, "SELECT count(*) FROM dim_merchant WHERE is_unknown") == [(1,)]


def test_missing_silver_table_fails_the_run(layout):
    silver_dir, _, observability_dir = layout
    for path in (silver_dir / "merchants").rglob("*"):
        if path.is_file():
            path.unlink()

    with pytest.raises(SystemExit):
        build_gold.main([])

    assert "merchants" in read(observability_dir, PIPELINE_RUN).to_pylist()[0]["error"]


@pytest.mark.parametrize(
    "argv",
    [
        ["--reference-date", "2025-03-16", "--start-date", "2025-03-01"],
        ["--end-date", "2025-03-01"],
        ["--reference-date", "2025/03/16"],
    ],
)
def test_invalid_date_arguments_are_rejected(argv):
    with pytest.raises(SystemExit):
        build_gold.parse_args(argv)


def test_low_volume_date_is_not_compared_with_the_baseline(layout):
    silver_dir, gold_dir, observability_dir = layout
    extra = [
        result(run_id, reference_date, f"x{run_id}{n}", "MATCHED", 10, 10, "M2", f"{reference_date}T03:00:00", 100 + n)
        for run_id, reference_date in ((1, "2025-03-10"), (3, "2025-03-13"), (4, "2025-03-16"))
        for n in range(3)
    ]
    write_silver(silver_dir, "reconciliation_results", SILVER["reconciliation_results"] + extra)

    build_gold.main([])

    assert query(gold_dir, """
        SELECT reference_date, result_count, result_count_baseline, is_comparable_volume
        FROM agg_reconciliation_daily WHERE result_count > 0 ORDER BY reference_date
    """) == [
        (date(2025, 3, 10), 5, None, True),
        (date(2025, 3, 13), 5, 5.0, True),
        (date(2025, 3, 16), 6, 5.0, True),
        (date(2025, 3, 22), 1, 5.0, False),
    ]
    metrics = {r["metric"] for r in read(observability_dir, PIPELINE_METRICS).to_pylist()}
    assert "discrepancy_rate" in metrics
    assert "discrepancy_rate_deviation" not in metrics

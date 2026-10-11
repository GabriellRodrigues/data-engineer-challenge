import duckdb
import pyarrow as pa
import pytest

from src.data_contract.contract_file_validator import validate_contract
from src.data_quality.checks import (
    Check,
    blocking,
    contract_checks,
    evaluate,
    load_checks,
    not_null_check,
    run_checks,
    unique_check,
)
from src.observability.store import DQ_RESULTS, read


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.register("orders", pa.table({
        "order_id": ["O-1", "O-2", "O-2", "O-3", "O-3", "O-3"],
        "line": [1, 1, 1, 1, 2, 2],
        "note": ["a", None, "b", None, None, "c"],
    }))
    return connection


def test_unique_counts_extra_rows_per_duplicated_key(con):
    assert evaluate(con, unique_check("orders", ["order_id"]))[1] == 3
    assert evaluate(con, unique_check("orders", ["order_id", "line"]))[1] == 2


def test_unique_on_empty_table_passes():
    connection = duckdb.connect()
    connection.register("orders", pa.table({"order_id": pa.array([], pa.string())}))
    assert evaluate(connection, unique_check("orders", ["order_id"])) == ("pass", 0, "0 failing rows (threshold 0)")


def test_not_null_counts_null_rows(con):
    assert evaluate(con, not_null_check("orders", "note"))[:2] == ("fail", 3)


def test_quoted_identifiers_allow_special_column_names():
    connection = duckdb.connect()
    connection.register("t", pa.table({"check": [None, "x"], "a b": ["1", "1"]}))
    assert evaluate(connection, not_null_check("t", "check"))[1] == 1
    assert evaluate(connection, unique_check("t", ["a b"]))[1] == 1


@pytest.mark.parametrize(
    ("severity", "threshold", "expected"),
    [("error", 0, "fail"), ("warn", 0, "warn"), ("error", 3, "pass"), ("warn", 2, "warn")],
)
def test_status_depends_on_severity_and_threshold(con, severity, threshold, expected):
    check = Check("c", "orders", severity, "SELECT count(*) FROM orders WHERE note IS NULL", threshold=threshold)
    assert evaluate(con, check)[0] == expected


def test_null_query_result_counts_as_zero(con):
    assert evaluate(con, Check("c", "orders", "error", "SELECT NULL"))[:2] == ("pass", 0)


def test_broken_query_fails_without_raising(con):
    status, failing_rows, message = evaluate(con, Check("c", "missing", "warn", "SELECT count(*) FROM missing"))
    assert (status, failing_rows) == ("fail", None)
    assert message.startswith("check query failed")


def test_contract_checks_cover_required_columns_and_primary_key():
    contract = validate_contract({
        "apiVersion": "v3.0.2", "kind": "DataContract", "id": "t.o", "name": "orders",
        "version": "1.0.0", "status": "active",
        "schema": [{"name": "orders", "properties": [
            {"name": "line", "logicalType": "integer", "physicalType": "bigint", "required": True,
             "primaryKey": True, "primaryKeyPosition": 2},
            {"name": "order_id", "logicalType": "string", "physicalType": "varchar", "required": True,
             "primaryKey": True, "primaryKeyPosition": 1},
            {"name": "note", "logicalType": "string", "physicalType": "varchar", "required": False},
        ]}],
    })

    names = [check.name for check in contract_checks("orders", contract)]

    assert names == ["not_null:orders:line", "not_null:orders:order_id", "unique:orders:order_id+line"]


def test_load_checks_reads_yaml_and_validates_severity(tmp_path):
    path = tmp_path / "checks.yml"
    path.write_text(
        "checks:\n"
        "  - {name: c1, table: orders, severity: warn, query: SELECT 0, threshold: 2, type: expression}\n",
        encoding="utf-8",
    )
    assert load_checks(path) == [Check("c1", "orders", "warn", "SELECT 0", "expression", 2)]

    path.write_text("checks:\n  - {name: c1, table: orders, severity: fatal, query: SELECT 0}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="severity must be one of"):
        load_checks(path)


def test_load_checks_without_checks_returns_empty(tmp_path):
    path = tmp_path / "checks.yml"
    path.write_text("checks:\n", encoding="utf-8")
    assert load_checks(path) == []


def test_run_checks_persists_results_and_reports_blocking(con, tmp_path):
    checks = [
        unique_check("orders", ["order_id"]),
        not_null_check("orders", "note", severity="warn"),
        Check("ok", "orders", "error", "SELECT 0"),
    ]

    results = run_checks(con, checks, "run-1", "silver", tmp_path)

    stored = read(tmp_path, DQ_RESULTS).select(["run_id", "check_name", "status", "failing_rows"]).to_pylist()
    assert stored == [
        {"run_id": "run-1", "check_name": "unique:orders:order_id", "status": "fail", "failing_rows": 3},
        {"run_id": "run-1", "check_name": "not_null:orders:note", "status": "warn", "failing_rows": 3},
        {"run_id": "run-1", "check_name": "ok", "status": "pass", "failing_rows": 0},
    ]
    assert blocking(results) == ["unique:orders:order_id"]


def test_run_checks_without_checks_writes_nothing(con, tmp_path):
    assert run_checks(con, [], "run-1", "silver", tmp_path) == []
    assert not (tmp_path / DQ_RESULTS).exists()

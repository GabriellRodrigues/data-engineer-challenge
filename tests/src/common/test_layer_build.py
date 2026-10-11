from datetime import date

import pyarrow as pa
import pytest

from src.common import layer_build
from src.common.layer_build import Layer, LayerTable
from src.data_contract.schema_validator import SchemaContractError
from src.observability.store import PIPELINE_RUN, read

CONTRACT = {
    "name": "orders",
    "version": "1.0.0",
    "schema": [{
        "name": "orders",
        "properties": [
            {"name": "order_id", "physicalType": "bigint", "required": True},
            {"name": "amount", "physicalType": "decimal(18,2)", "required": True},
            {"name": "note", "physicalType": "varchar", "required": False},
        ],
    }],
}


def layer(tmp_path, tables) -> Layer:
    return Layer("gold", "silver", tables, tmp_path, tmp_path, tmp_path / "checks.yml", tmp_path / "out", tmp_path)


def test_scope_covers_the_lookback_window():
    table = LayerTable("facts", ("a",), "created_date", lookback_days=7)

    assert table.scope(date(2025, 3, 16), date(2025, 3, 17)) == (
        "created_date >= '2025-03-09' AND created_date <= '2025-03-17'"
    )


@pytest.mark.parametrize(
    "table, start, end",
    [
        (LayerTable("dims", ("a",)), date(2025, 3, 16), date(2025, 3, 16)),
        (LayerTable("facts", ("a",), "reference_date"), None, None),
    ],
)
def test_scope_is_full_without_partition_or_dates(table, start, end):
    assert table.scope(start, end) is None


def test_layer_lists_inputs_once_and_hides_unpublished_tables(tmp_path):
    tables = (
        LayerTable("helper", ("b", "a"), publish=False),
        LayerTable("facts", ("a", "c")),
        LayerTable("dims", ()),
    )
    current = layer(tmp_path, tables)

    assert current.inputs() == ["a", "b", "c"]
    assert [t.name for t in current.published] == ["facts", "dims"]


def test_conform_orders_columns_and_fills_optional_ones():
    data = pa.table({"amount": pa.array([1], pa.decimal128(18, 2)), "order_id": pa.array([7], pa.int64())})

    conformed = layer_build.conform(data, CONTRACT)

    assert conformed.column_names == ["order_id", "amount", "note"]
    assert conformed["note"].to_pylist() == [None]


def test_conform_refuses_undeclared_columns():
    data = pa.table({"order_id": [1], "amount": pa.array([1], pa.decimal128(18, 2)), "secret": ["x"]})

    with pytest.raises(SchemaContractError, match="secret"):
        layer_build.conform(data, CONTRACT)


def test_conform_refuses_wrong_types():
    data = pa.table({"order_id": ["1"], "amount": pa.array([1], pa.decimal128(18, 2))})

    with pytest.raises(SchemaContractError, match="order_id"):
        layer_build.conform(data, CONTRACT)


def test_connect_works_without_macros_and_loads_them_when_present(tmp_path):
    assert layer_build.connect(tmp_path).sql("SELECT current_setting('TimeZone')").fetchone() == ("UTC",)

    (tmp_path / "macros.sql").write_text("CREATE OR REPLACE MACRO twice(x) AS x * 2;")
    assert layer_build.connect(tmp_path).sql("SELECT twice(21)").fetchone() == (42,)


def test_compute_stamps_the_layer_run(tmp_path):
    (tmp_path / "facts.sql").write_text("SELECT 1 AS id")
    con = layer_build.connect(tmp_path)

    class Run:
        run_id = "it's-1"

    rows = layer_build.compute(con, layer(tmp_path, ()), LayerTable("facts", ()), Run())

    assert rows == 1
    assert con.sql("SELECT _gold_run_id FROM facts").fetchone() == ("it's-1",)


@pytest.mark.parametrize(
    "argv",
    [
        ["--reference-date", "2025-03-16", "--end-date", "2025-03-17"],
        ["--start-date", "2025-03-02"],
        ["--start-date", "2025-03-10", "--end-date", "2025-03-01"],
        ["--reference-date", "2025-02-30"],
    ],
)
def test_invalid_dates_are_rejected(argv):
    with pytest.raises(SystemExit):
        layer_build.parse_date_args("test", argv)


def test_dates_resolve_to_a_range():
    single = layer_build.parse_date_args("test", ["--reference-date", "2025-03-16"])
    window = layer_build.parse_date_args("test", ["--start-date", "2025-03-01", "--end-date", "2025-03-16"])

    assert (single.start, single.end) == (date(2025, 3, 16), date(2025, 3, 16))
    assert (window.start, window.end) == (date(2025, 3, 1), date(2025, 3, 16))


def test_run_job_records_a_failed_build_and_exits(tmp_path):
    rules = tmp_path / "rules.yml"
    rules.write_text("rules: []\n")

    def broken(run, start, end):
        raise ValueError("boom")

    with pytest.raises(SystemExit) as exit_info:
        layer_build.run_job("test.job", tmp_path / "out", tmp_path / "obs", rules, broken, None, None)

    assert exit_info.value.code == 1
    assert read(tmp_path / "obs", PIPELINE_RUN).select(["job", "mode", "status", "error"]).to_pylist() == [
        {"job": "test.job", "mode": "full", "status": "failed", "error": "ValueError: boom"}
    ]


def test_run_job_succeeds_in_date_mode(tmp_path):
    rules = tmp_path / "rules.yml"
    rules.write_text("rules: []\n")
    seen = []

    layer_build.run_job(
        "test.job", tmp_path / "out", tmp_path / "obs", rules,
        lambda run, start, end: seen.append((start, end)), date(2025, 3, 16), date(2025, 3, 16),
    )

    assert seen == [(date(2025, 3, 16), date(2025, 3, 16))]
    assert (tmp_path / "out").is_dir()
    assert read(tmp_path / "obs", PIPELINE_RUN).select(["mode", "status"]).to_pylist() == [
        {"mode": "reference_date", "status": "success"}
    ]

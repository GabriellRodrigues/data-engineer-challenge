import importlib.util
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
from deltalake import write_deltalake

from etl_jobs.gold import build_gold, run_queries
from src.utils.paths import GOLD_QUERIES_DIR

BUILD_TESTS = Path(__file__).with_name("test_build_gold.py")


def load_build_fixtures():
    spec = importlib.util.spec_from_file_location("gold_build_fixtures", BUILD_TESTS)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gold_dir(tmp_path_factory):
    fixtures = load_build_fixtures()
    with pytest.MonkeyPatch.context() as monkeypatch:
        _, gold, _ = fixtures.prepare(tmp_path_factory.mktemp("gold_queries"), monkeypatch)
        build_gold.main([])
        yield gold


SHIPPED = run_queries.discover(GOLD_QUERIES_DIR)


def test_every_consumer_has_queries():
    consumers = {path.parent.name for path in SHIPPED}
    assert consumers == set(run_queries.CONSUMERS)


@pytest.mark.parametrize("path", SHIPPED, ids=lambda p: f"{p.parent.name}/{p.stem}")
def test_shipped_query_runs_against_gold(gold_dir, path):
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    run_queries.register_gold(con, gold_dir)

    columns, _ = run_queries.run_query(con, path)

    assert columns


def test_key_questions_return_answers(gold_dir):
    con = duckdb.connect()
    run_queries.register_gold(con, gold_dir)

    _, trend = run_queries.run_query(con, GOLD_QUERIES_DIR / "operations" / "01_discrepancy_trend.sql")
    _, trail = run_queries.run_query(con, GOLD_QUERIES_DIR / "compliance" / "01_transaction_trail.sql")
    _, tpv = run_queries.run_query(con, GOLD_QUERIES_DIR / "cfo" / "01_daily_tpv.sql")

    assert [row[0].isoformat() for row in trend] == ["2025-03-10", "2025-03-13", "2025-03-16", "2025-03-22"]
    assert {row[2] for row in trail} >= {"cdc I", "cdc U", "verdict run 1", "verdict run 3"}
    assert sum(row[3] for row in tpv) == 350


def test_discover_filters_by_consumer():
    paths = run_queries.discover(GOLD_QUERIES_DIR, "cfo")

    assert paths
    assert {path.parent.name for path in paths} == {"cfo"}
    assert paths == sorted(paths)


def test_markdown_truncates_and_escapes():
    text = run_queries.to_markdown(["a", "b"], [("x|y", None), (0.123456, 2), (3, 4)], max_rows=2)

    assert text.splitlines()[:4] == ["| a | b |", "|---|---|", "| x\\|y | - |", "| 0.1235 | 2 |"]
    assert "1 more rows not shown" in text


def test_missing_gold_layer_is_reported(tmp_path):
    with pytest.raises(FileNotFoundError):
        run_queries.register_gold(duckdb.connect(), tmp_path / "gold")


def test_failing_query_exits_with_error(tmp_path, monkeypatch):
    gold = tmp_path / "gold"
    write_deltalake(gold / "dim_category", pa.table({"category": ["MATCHED"]}))
    queries = tmp_path / "queries"
    (queries / "operations").mkdir(parents=True)
    (queries / "operations" / "01_ok.sql").write_text("SELECT count(*) AS n FROM dim_category")
    (queries / "operations" / "02_broken.sql").write_text("SELECT * FROM missing_table")
    output = tmp_path / "out" / "queries.md"
    monkeypatch.setattr(run_queries, "GOLD_DIR", gold)
    monkeypatch.setattr(run_queries, "GOLD_QUERIES_DIR", queries)

    with pytest.raises(SystemExit) as exit_info:
        run_queries.main(["--output", str(output)])

    assert exit_info.value.code == 1
    assert "## operations/01_ok" in output.read_text(encoding="utf-8")
    assert "02_broken" not in output.read_text(encoding="utf-8")

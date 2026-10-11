from src.lineage.graph import Lineage, Node
from src.observability.alerts import Rule, evaluate_alerts
from src.observability.report import build_report, stale_tables
from src.observability.run_registry import pipeline_run

REFUSED_WARN = Rule(name="refused_elevated", type="threshold", metric="refused_rate",
                    operator=">", value=0.01, severity="warn")


def record_ingest(base_dir, refused_rate: float) -> str:
    with pipeline_run("bronze.ingest", "ingest", base_dir) as run:
        with run.step("ingest:orders.csv", "bronze", "orders") as step:
            step.rows_in, step.rows_out, step.rows_rejected = 100, 95, 5
        run.metric("bronze", "orders", "refused_rate", refused_rate, source_file="orders.csv")
        evaluate_alerts(run, [REFUSED_WARN])
    return run.run_id


def record_maintenance(base_dir) -> str:
    with (
        pipeline_run("maintenance.optimize", "maintenance", base_dir) as run,
        run.step("optimize:orders", "bronze", "orders"),
    ):
        pass
    return run.run_id


def test_report_without_runs(tmp_path):
    run_ids, report = build_report(tmp_path)

    assert run_ids == []
    assert "No run found" in report


def test_report_shows_latest_run_of_each_job(tmp_path):
    record_ingest(tmp_path, 0.0)
    ingest = record_ingest(tmp_path, 0.05)
    maintenance = record_maintenance(tmp_path)

    run_ids, report = build_report(tmp_path)

    assert run_ids == [maintenance, ingest]
    assert f"## bronze.ingest — run `{ingest}`" in report
    assert f"## maintenance.optimize — run `{maintenance}`" in report
    assert "- **Status:** SUCCESS" in report
    assert "| ingest:orders.csv | success | 100 | 95 | 5 |" in report
    assert "| warn | refused_elevated | orders | orders.csv |" in report
    assert "| bronze | orders | orders.csv | refused_rate | 0.05 | 0 | 1 |" in report
    assert "## Last 10 runs" in report


def test_report_for_specific_run(tmp_path):
    first = record_ingest(tmp_path, 0.0)
    record_ingest(tmp_path, 0.05)

    run_ids, report = build_report(tmp_path, run_id=first)

    assert run_ids == [first]
    assert "No warnings or failures (1 rule evaluations passed)." in report


def small_lineage() -> Lineage:
    lineage = Lineage()
    for node in (
        Node("bronze", "orders"),
        Node("silver", "orders"),
        Node("silver", "orders_dedup", published=False),
        Node("gold", "fact_orders", critical=True),
        Node("gold", "agg_orders"),
    ):
        lineage.add(node)
    lineage.link("bronze.orders", "silver.orders")
    lineage.link("silver.orders", "gold.fact_orders")
    lineage.link("gold.fact_orders", "gold.agg_orders")
    return lineage


def record_failed_silver(base_dir) -> str:
    with pipeline_run("silver.build", "full", base_dir) as run:
        with run.step("compute:orders_dedup", "silver", "orders_dedup"):
            pass
        with run.step("compute:orders", "silver", "orders"):
            pass
        with run.step("quality:silver", "silver"):
            pass
        run.mark_failed("Blocking data quality checks failed: unique:orders")
    return run.run_id


def test_stale_tables_are_failed_steps_and_computed_but_unpublished_tables():
    steps = [
        ("ingest:a.csv", "bronze", "a", "failed"),
        ("ingest:b.csv", "bronze", "b", "success"),
        ("compute:c", "silver", "c", "success"),
        ("publish:c", "silver", "c", "success"),
        ("compute:d", "silver", "d", "success"),
        ("publish:e", "silver", "e", "failed"),
        ("optimize:f", "silver", "f", "failed"),
        ("quality:silver", "silver", None, "success"),
    ]

    assert stale_tables(steps) == {"bronze.a", "silver.d", "silver.e"}


def test_failed_run_lists_the_critical_tables_left_stale(tmp_path):
    run_id = record_failed_silver(tmp_path)

    _, report = build_report(tmp_path, run_id=run_id, lineage=small_lineage())

    assert "### Critical tables affected" in report
    assert "| silver.orders | gold.fact_orders |" in report
    assert "| silver.orders_dedup |" not in report


def test_critical_table_itself_counts_as_affected(tmp_path):
    with pipeline_run("gold.build", "full", tmp_path) as run:
        with run.step("compute:fact_orders", "gold", "fact_orders"):
            pass
        run.mark_failed("boom")

    _, report = build_report(tmp_path, run_id=run.run_id, lineage=small_lineage())

    assert "| gold.fact_orders | gold.fact_orders |" in report


def test_successful_run_or_missing_lineage_has_no_impact_section(tmp_path):
    ok = record_ingest(tmp_path, 0.0)
    failed = record_failed_silver(tmp_path)

    _, ok_report = build_report(tmp_path, run_id=ok, lineage=small_lineage())
    _, no_lineage = build_report(tmp_path, run_id=failed)

    assert "Critical tables affected" not in ok_report
    assert "Critical tables affected" not in no_lineage

from src.observability.alerts import Rule, evaluate_alerts
from src.observability.report import build_report
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

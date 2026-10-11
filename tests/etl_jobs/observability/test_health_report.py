import pytest

from etl_jobs.observability import health_report
from src.observability.run_registry import pipeline_run


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    base_dir = tmp_path / "observability"
    reports_dir = base_dir / "reports"
    monkeypatch.setattr(health_report, "OBSERVABILITY_DIR", base_dir)
    monkeypatch.setattr(health_report, "OBSERVABILITY_REPORTS_DIR", reports_dir)
    return base_dir, reports_dir


def test_report_is_written_with_latest_copy(dirs):
    base_dir, reports_dir = dirs
    with pipeline_run("bronze.ingest", "ingest", base_dir) as run:
        pass

    health_report.main([])

    latest = (reports_dir / "latest.md").read_text(encoding="utf-8")
    [timestamped] = [p for p in reports_dir.glob("health_*.md")]
    assert timestamped.read_text(encoding="utf-8") == latest
    assert f"run `{run.run_id}`" in latest


def test_report_for_specific_run_does_not_replace_latest(dirs):
    base_dir, reports_dir = dirs
    with pipeline_run("bronze.ingest", "ingest", base_dir) as run:
        pass

    health_report.main(["--run-id", run.run_id])

    assert (reports_dir / f"health_{run.run_id}.md").exists()
    assert not (reports_dir / "latest.md").exists()


def test_missing_run_fails(dirs):
    with pytest.raises(SystemExit) as exit_info:
        health_report.main([])

    assert exit_info.value.code == 1


def test_failed_run_report_names_critical_tables(dirs):
    base_dir, reports_dir = dirs
    with pipeline_run("gold.build", "full", base_dir) as run:
        with run.step("compute:fact_transaction", "gold", "fact_transaction"):
            pass
        run.mark_failed("Blocking data quality checks failed")

    health_report.main([])

    latest = (reports_dir / "latest.md").read_text(encoding="utf-8")
    assert "| gold.fact_transaction | gold.fact_transaction, gold.agg_volume_daily |" in latest


def test_report_survives_a_broken_lineage(dirs, monkeypatch):
    base_dir, reports_dir = dirs
    with pipeline_run("gold.build", "full", base_dir) as run:
        run.mark_failed("boom")

    def broken():
        raise ValueError("bad contract")

    monkeypatch.setattr(health_report, "build_lineage", broken)
    health_report.main([])

    assert "Critical tables affected" not in (reports_dir / "latest.md").read_text(encoding="utf-8")

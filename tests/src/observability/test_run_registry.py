import re

import pyarrow as pa
import pytest
from deltalake import write_deltalake

from src.observability import run_registry
from src.observability.run_registry import pipeline_run
from src.observability.store import PIPELINE_METRICS, PIPELINE_RUN, PIPELINE_RUN_STEP, read


def rows(base_dir, table_name: str, columns: list[str]) -> list[dict]:
    return read(base_dir, table_name).select(columns).to_pylist()


def test_successful_run_records_run_steps_and_metrics(tmp_path, monkeypatch):
    monkeypatch.setenv("GIT_SHA", "abc1234")
    output = tmp_path / "data" / "orders"

    with pipeline_run("test.job", "ingest", tmp_path) as run:
        with run.step("load", "bronze", "orders", output, ["source.csv"], "source.csv") as step:
            write_deltalake(output, pa.table({"id": [1, 2]}))
            step.rows_in, step.rows_out, step.rows_rejected = 3, 2, 1
        run.metric("bronze", "orders", "rows_read", 3, source_file="source.csv")

    assert rows(tmp_path, PIPELINE_RUN, ["run_id", "job", "mode", "status", "git_sha", "error"]) == [
        {"run_id": run.run_id, "job": "test.job", "mode": "ingest", "status": "success",
         "git_sha": "abc1234", "error": None}
    ]
    assert rows(tmp_path, PIPELINE_RUN_STEP, [
        "step", "status", "inputs", "version_before", "version_after", "rows_in", "rows_out", "rows_rejected",
    ]) == [{
        "step": "load", "status": "success", "inputs": ["source.csv"], "version_before": None,
        "version_after": 0, "rows_in": 3, "rows_out": 2, "rows_rejected": 1,
    }]
    assert rows(tmp_path, PIPELINE_METRICS, ["run_id", "table_name", "source_file", "metric", "value"]) == [
        {"run_id": run.run_id, "table_name": "orders", "source_file": "source.csv",
         "metric": "rows_read", "value": 3.0}
    ]


def test_failed_step_is_recorded_and_reraised(tmp_path):
    with (
        pytest.raises(ValueError, match="boom"),
        pipeline_run("test.job", "ingest", tmp_path) as run,
        run.step("load", "bronze"),
    ):
        raise ValueError("boom")

    assert rows(tmp_path, PIPELINE_RUN_STEP, ["status", "error"]) == [
        {"status": "failed", "error": "ValueError: boom"}
    ]
    assert rows(tmp_path, PIPELINE_RUN, ["run_id", "status", "error"]) == [
        {"run_id": run.run_id, "status": "failed", "error": "ValueError: boom"}
    ]


def test_run_can_be_marked_failed_without_exception(tmp_path):
    with pipeline_run("test.job", "ingest", tmp_path) as run:
        run.mark_failed("2 files failed")

    assert rows(tmp_path, PIPELINE_RUN, ["status", "error"]) == [{"status": "failed", "error": "2 files failed"}]


def test_step_captures_delta_version_change(tmp_path):
    output = tmp_path / "orders"
    write_deltalake(output, pa.table({"id": [1]}))

    with (
        pipeline_run("test.job", "ingest", tmp_path) as run,
        run.step("append", "bronze", output_table=output),
    ):
        write_deltalake(output, pa.table({"id": [2]}), mode="append")

    assert rows(tmp_path, PIPELINE_RUN_STEP, ["version_before", "version_after"]) == [
        {"version_before": 0, "version_after": 1}
    ]


def test_git_sha_defaults_to_unknown(tmp_path, monkeypatch):
    monkeypatch.delenv("GIT_SHA", raising=False)
    with pipeline_run("test.job", "ingest", tmp_path):
        pass

    assert rows(tmp_path, PIPELINE_RUN, ["git_sha"]) == [{"git_sha": "unknown"}]


def test_run_and_step_durations_are_logged(tmp_path, caplog):
    with (
        caplog.at_level("INFO"),
        pipeline_run("test.job", "ingest", tmp_path) as run,
        run.step("load", "bronze") as step,
    ):
        step.status = "skipped"

    messages = [r.getMessage() for r in caplog.records if r.name == "observability.run"]
    assert re.fullmatch(r"Step load finished with status skipped in \d+\.\d{2}s", messages[1])
    assert re.fullmatch(
        rf"Run {run.run_id} \(test\.job\) finished with status success in \d+\.\d{{2}}s \(1 steps\)",
        messages[2],
    )


def test_consecutive_steps_track_versions_of_the_same_table(tmp_path):
    output = tmp_path / "orders"

    with pipeline_run("test.job", "ingest", tmp_path) as run:
        for values in ([1], [2]):
            with run.step(f"append:{values[0]}", "bronze", output_table=output):
                write_deltalake(output, pa.table({"id": values}), mode="append")
        with run.step("skip", "bronze", output_table=output) as step:
            step.status = "skipped"

    versions = read(tmp_path, PIPELINE_RUN_STEP).select(["step", "version_before", "version_after"])
    assert sorted(versions.to_pylist(), key=lambda r: r["step"]) == [
        {"step": "append:1", "version_before": None, "version_after": 0},
        {"step": "append:2", "version_before": 0, "version_after": 1},
        {"step": "skip", "version_before": 1, "version_after": 1},
    ]


def test_skipped_steps_do_not_reload_the_table(tmp_path, monkeypatch):
    output = tmp_path / "orders"
    write_deltalake(output, pa.table({"id": [1]}))
    calls = []
    original = run_registry.delta_version
    monkeypatch.setattr(run_registry, "delta_version", lambda path: calls.append(path) or original(path))

    with pipeline_run("test.job", "ingest", tmp_path) as run:
        for n in range(5):
            with run.step(f"skip:{n}", "bronze", output_table=output) as step:
                step.status = "skipped"

    assert calls == [output]

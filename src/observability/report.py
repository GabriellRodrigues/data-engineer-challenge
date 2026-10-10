from datetime import UTC, datetime
from pathlib import Path

import duckdb

from src.observability.store import SCHEMAS, read

TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
BASELINE_WINDOW = 7


def _connect(base_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    for table_name in SCHEMAS:
        con.register(table_name, read(base_dir, table_name))
    return con


def _fmt(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:,.4g}" if abs(value) < 1 else f"{value:,.2f}".rstrip("0").rstrip(".")
    text = str(value).replace("|", "\\|").replace("\n", " ")
    return text if len(text) <= 120 else text[:117] + "..."


def _table(headers: list[str], rows: list[tuple]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    lines += ["| " + " | ".join(_fmt(value) for value in row) + " |" for row in rows]
    return lines


def _ts(column: str) -> str:
    return f"strftime({column}::TIMESTAMP, '{TIME_FORMAT}')"


RUN_COLUMNS = f"run_id, job, mode, status, {_ts('started_at')}, round(duration_s, 2), git_sha, error"


def _select_runs(con: duckdb.DuckDBPyConnection, run_id: str | None) -> list[tuple]:
    if run_id:
        return con.execute(f"SELECT {RUN_COLUMNS} FROM pipeline_run WHERE run_id = ?", [run_id]).fetchall()
    return con.execute(
        f"""
        SELECT {RUN_COLUMNS} FROM pipeline_run
        QUALIFY row_number() OVER (PARTITION BY job ORDER BY started_at DESC) = 1
        ORDER BY started_at DESC
        """
    ).fetchall()


def _run_section(con: duckdb.DuckDBPyConnection, run: tuple) -> list[str]:
    run_id, job, mode, status, started_at, duration_s, git_sha, error = run
    lines = [
        f"## {job} — run `{run_id}`",
        "",
        f"- **Mode:** {mode}",
        f"- **Status:** {status.upper()}",
        f"- **Started:** {started_at} UTC, took {duration_s}s",
        f"- **Commit:** {git_sha}",
    ]
    if error:
        lines.append(f"- **Error:** {_fmt(error)}")

    steps = con.execute(
        "SELECT step, status, rows_in, rows_out, rows_rejected, "
        "concat(coalesce(version_before::VARCHAR, 'new'), ' -> ', coalesce(version_after::VARCHAR, '-')), "
        "round(duration_s, 2), error FROM pipeline_run_step WHERE run_id = ? ORDER BY started_at",
        [run_id],
    ).fetchall()
    lines += ["", "### Steps", ""]
    lines += _table(["Step", "Status", "Rows in", "Rows out", "Refused", "Delta version", "Seconds", "Error"], steps)

    alerts = con.execute(
        "SELECT status, rule, table_name, source_file, message FROM pipeline_alerts "
        "WHERE run_id = ? AND status <> 'pass' ORDER BY status, rule, table_name",
        [run_id],
    ).fetchall()
    evaluated = con.execute("SELECT count(*) FROM pipeline_alerts WHERE run_id = ?", [run_id]).fetchone()[0]
    lines += ["", "### Alerts", ""]
    if alerts:
        lines += _table(["Status", "Rule", "Table", "Source file", "Detail"], alerts)
    else:
        lines.append(f"No warnings or failures ({evaluated} rule evaluations passed).")

    metrics = con.execute(
        f"""
        WITH current AS (
            SELECT layer, table_name, source_file, metric, value
            FROM pipeline_metrics WHERE run_id = ?
        ),
        history AS (
            SELECT m.layer, m.table_name, m.metric, m.value,
                   row_number() OVER (PARTITION BY m.layer, m.table_name, m.metric ORDER BY r.started_at DESC) AS rn
            FROM pipeline_metrics m JOIN pipeline_run r USING (run_id)
            WHERE r.started_at < (SELECT started_at FROM pipeline_run WHERE run_id = ?)
        ),
        baseline AS (
            SELECT layer, table_name, metric, median(value) AS median_value, count(*) AS runs
            FROM history WHERE rn <= {BASELINE_WINDOW} GROUP BY ALL
        )
        SELECT c.layer, c.table_name, c.source_file, c.metric, c.value, b.median_value, b.runs
        FROM current c LEFT JOIN baseline b USING (layer, table_name, metric)
        ORDER BY c.layer, c.table_name, c.source_file NULLS LAST, c.metric
        """,
        [run_id, run_id],
    ).fetchall()
    lines += ["", f"### Metrics (vs median of the previous {BASELINE_WINDOW} values)", ""]
    lines += _table(["Layer", "Table", "Source file", "Metric", "Value", "Median", "History"], metrics)
    return lines + [""]


def build_report(base_dir: Path, run_id: str | None = None, last: int = 10) -> tuple[list[str], str]:
    con = _connect(base_dir)
    lines = ["# Pipeline health report", "", f"Generated at {datetime.now(UTC).strftime(TIME_FORMAT)} UTC.", ""]

    runs = _select_runs(con, run_id)
    if not runs:
        lines.append(f"No run found{f' with run_id {run_id}' if run_id else ''}.")
        return [], "\n".join(lines) + "\n"

    if not run_id:
        lines += ["Latest run of each job.", ""]
    for run in runs:
        lines += _run_section(con, run)

    recent = con.execute(
        f"""
        SELECT r.run_id, r.job, r.mode, r.status, {_ts('r.started_at')}, round(r.duration_s, 2),
               count(s.step) FILTER (WHERE s.status = 'failed'),
               coalesce(sum(s.rows_rejected), 0)
        FROM pipeline_run r LEFT JOIN pipeline_run_step s USING (run_id)
        GROUP BY r.run_id, r.job, r.mode, r.status, r.started_at, r.duration_s
        ORDER BY r.started_at DESC LIMIT ?
        """,
        [last],
    ).fetchall()
    lines += [f"## Last {last} runs", ""]
    lines += _table(["Run", "Job", "Mode", "Status", "Started (UTC)", "Seconds", "Failed steps", "Refused rows"], recent)

    return [run[0] for run in runs], "\n".join(lines) + "\n"

import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path

from deltalake import DeltaTable

from src.observability.store import PIPELINE_METRICS, PIPELINE_RUN, PIPELINE_RUN_STEP, append
from src.utils.logger import get_logger

logger = get_logger("observability.run")

SUCCESS = "success"
FAILED = "failed"
SKIPPED = "skipped"


def _now() -> datetime:
    return datetime.now(UTC)


def _describe(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}"


def delta_version(path: Path | None) -> int | None:
    if path is None or not (path / "_delta_log").is_dir():
        return None
    return DeltaTable(str(path)).version()


@dataclass
class StepContext:
    step: str
    layer: str
    dataset: str | None
    output_table: Path | None
    inputs: list[str]
    source_file: str | None
    contract_version: str | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    rows_rejected: int | None = None
    status: str = SUCCESS


@dataclass
class RunContext:
    job: str
    mode: str
    base_dir: Path
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    git_sha: str = field(default_factory=lambda: os.getenv("GIT_SHA") or "unknown")
    started_at: datetime = field(default_factory=_now)
    status: str = SUCCESS
    error: str | None = None
    steps: list[dict] = field(default_factory=list)
    metrics: list[dict] = field(default_factory=list)
    versions: dict[Path, int | None] = field(default_factory=dict)

    def table_version(self, path: Path | None, refresh: bool = False) -> int | None:
        if path is None:
            return None
        if refresh or path not in self.versions:
            self.versions[path] = delta_version(path)
        return self.versions[path]

    def mark_failed(self, error: str) -> None:
        self.status = FAILED
        self.error = error

    def metric(
        self,
        layer: str,
        table_name: str,
        metric: str,
        value: float,
        source_file: str | None = None,
        reference_date: date | None = None,
    ) -> None:
        self.metrics.append({
            "run_id": self.run_id,
            "layer": layer,
            "table_name": table_name,
            "source_file": source_file,
            "metric": metric,
            "value": float(value),
            "reference_date": reference_date,
            "computed_at": _now(),
        })

    @contextmanager
    def step(
        self,
        step: str,
        layer: str,
        dataset: str | None = None,
        output_table: Path | None = None,
        inputs: list[str] | None = None,
        source_file: str | None = None,
    ) -> Iterator[StepContext]:
        context = StepContext(step, layer, dataset, output_table, list(inputs or []), source_file)
        started_at = _now()
        version_before = self.table_version(output_table)
        error = None
        try:
            yield context
        except Exception as exc:
            context.status = FAILED
            error = _describe(exc)
            raise
        finally:
            ended_at = _now()
            duration_s = (ended_at - started_at).total_seconds()
            if context.status == SKIPPED:
                version_after = version_before
            else:
                version_after = self.table_version(output_table, refresh=True)
            logger.info("Step %s finished with status %s in %.2fs", context.step, context.status, duration_s)
            self.steps.append({
                "run_id": self.run_id,
                "step": context.step,
                "layer": context.layer,
                "dataset": context.dataset,
                "source_file": context.source_file,
                "contract_version": context.contract_version,
                "inputs": context.inputs,
                "output_table": str(output_table) if output_table else None,
                "version_before": version_before,
                "version_after": version_after,
                "rows_in": context.rows_in,
                "rows_out": context.rows_out,
                "rows_rejected": context.rows_rejected,
                "status": context.status,
                "started_at": started_at,
                "ended_at": ended_at,
                "duration_s": duration_s,
                "error": error,
            })


@contextmanager
def pipeline_run(job: str, mode: str, base_dir: Path) -> Iterator[RunContext]:
    run = RunContext(job=job, mode=mode, base_dir=base_dir)
    logger.info("Run %s started (job=%s, mode=%s, git_sha=%s)", run.run_id, job, mode, run.git_sha)
    try:
        yield run
    except BaseException as exc:
        run.mark_failed(_describe(exc))
        raise
    finally:
        ended_at = _now()
        duration_s = (ended_at - run.started_at).total_seconds()
        append(base_dir, PIPELINE_RUN_STEP, run.steps)
        append(base_dir, PIPELINE_METRICS, run.metrics)
        append(base_dir, PIPELINE_RUN, [{
            "run_id": run.run_id,
            "job": job,
            "mode": mode,
            "status": run.status,
            "started_at": run.started_at,
            "ended_at": ended_at,
            "duration_s": duration_s,
            "git_sha": run.git_sha,
            "error": run.error,
        }])
        logger.info(
            "Run %s (%s) finished with status %s in %.2fs (%d steps)",
            run.run_id,
            job,
            run.status,
            duration_s,
            len(run.steps),
        )

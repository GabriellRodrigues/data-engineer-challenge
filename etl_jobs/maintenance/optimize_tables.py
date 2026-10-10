import sys
from pathlib import Path

from deltalake import DeltaTable

from src.common.delta_maintenance import maintain_table
from src.observability.run_registry import FAILED, SKIPPED, RunContext, pipeline_run
from src.observability.store import (
    PIPELINE_ALERTS,
    PIPELINE_METRICS,
    PIPELINE_RUN,
    PIPELINE_RUN_STEP,
)
from src.utils.logger import get_logger
from src.utils.paths import BRONZE_DIR, OBSERVABILITY_DIR

logger = get_logger("maintenance.optimize")

JOB_NAME = "maintenance.optimize"
MODE = "maintenance"

BRONZE_ZORDER = {
    "transactions": ["created_at", "transaction_id"],
    "transactions_refused": None,
    "settlement_paysettler": ["settled_at", "transaction_id"],
    "settlement_paysettler_refused": None,
    "reconciliation_results": ["run_id", "transaction_id"],
    "reconciliation_runs": None,
    "enterprise_company": None,
}

OBSERVABILITY_ZORDER = {
    PIPELINE_RUN: None,
    PIPELINE_RUN_STEP: None,
    PIPELINE_METRICS: None,
    PIPELINE_ALERTS: None,
}


def maintain(run: RunContext, layer: str, layer_dir: Path, tables: dict) -> list[str]:
    failed = []
    for table_name, zorder_columns in tables.items():
        path = layer_dir / table_name
        try:
            with run.step(f"optimize:{table_name}", layer, table_name, path, [str(path)]) as step:
                if not DeltaTable.is_deltatable(str(path)):
                    logger.info("Skipping %s: table not found", table_name)
                    step.status = SKIPPED
                    continue

                metrics = maintain_table(path, zorder_columns)
                for metric in ("files_added", "files_removed", "files_vacuumed"):
                    run.metric(layer, table_name, metric, metrics[metric])
                logger.info("Maintenance of %s completed: %s", table_name, metrics)
        except Exception:
            logger.exception("Maintenance of %s failed", table_name)
            failed.append(table_name)
    return failed


def main():
    with pipeline_run(JOB_NAME, MODE, OBSERVABILITY_DIR) as run:
        failed = []
        for layer, layer_dir, tables in (
            ("bronze", BRONZE_DIR, BRONZE_ZORDER),
            ("observability", OBSERVABILITY_DIR, OBSERVABILITY_ZORDER),
        ):
            failed += maintain(run, layer, layer_dir, tables)

        if failed:
            run.mark_failed(f"Maintenance failed for: {', '.join(failed)}")

    if run.status == FAILED:
        logger.error("Maintenance failed for: %s", ", ".join(failed))
        sys.exit(1)


if __name__ == "__main__":
    main()

import argparse
import sys
from datetime import UTC, datetime

from etl_jobs.observability.lineage import build_lineage
from src.lineage.graph import Lineage
from src.observability.report import build_report
from src.utils.logger import get_logger
from src.utils.paths import OBSERVABILITY_DIR, OBSERVABILITY_REPORTS_DIR

logger = get_logger("observability.report")

LATEST_REPORT = "latest.md"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the pipeline health report (Markdown)")
    parser.add_argument("--run-id", help="Report on this run only (default: latest run of each job)")
    parser.add_argument("--last", type=int, default=10, help="How many recent runs to list")
    return parser.parse_args(argv)


def load_lineage() -> Lineage | None:
    try:
        return build_lineage()
    except Exception:
        logger.exception("Lineage could not be built; the report will not list the critical tables affected")
        return None


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    run_ids, report = build_report(OBSERVABILITY_DIR, args.run_id, args.last, load_lineage())
    if not run_ids:
        logger.error("No pipeline run found to report on%s", f" (run_id={args.run_id})" if args.run_id else "")
        sys.exit(1)

    OBSERVABILITY_REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    if args.run_id:
        report_path = OBSERVABILITY_REPORTS_DIR / f"health_{args.run_id}.md"
    else:
        report_path = OBSERVABILITY_REPORTS_DIR / f"health_{datetime.now(UTC):%Y%m%d_%H%M%S}.md"
        (OBSERVABILITY_REPORTS_DIR / LATEST_REPORT).write_text(report, encoding="utf-8")
    report_path.write_text(report, encoding="utf-8")
    logger.info("Health report covering %d run(s) written to %s", len(run_ids), report_path)


if __name__ == "__main__":
    main()

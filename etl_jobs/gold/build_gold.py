from datetime import date

import duckdb
from deltalake import DeltaTable

from etl_jobs.gold.tables import GOLD_TABLES
from src.common.delta_io import read_at_version
from src.common.layer_build import Layer, build_layer, connect, parse_date_args, row_count, run_job
from src.observability.run_registry import RunContext
from src.utils.logger import get_logger
from src.utils.paths import (
    ALERT_RULES_FILE,
    GOLD_CHECKS_FILE,
    GOLD_CONTRACTS_DIR,
    GOLD_DIR,
    GOLD_SQL_DIR,
    OBSERVABILITY_DIR,
    SILVER_DIR,
)

logger = get_logger("gold.build")

JOB_NAME = "gold.build"
LAYER = "gold"
SILVER_VIEW_PREFIX = "silver_"
RECONCILIATION_TABLE = "agg_reconciliation_daily"


def gold_layer() -> Layer:
    return Layer(
        name=LAYER,
        input_layer="silver",
        tables=GOLD_TABLES,
        sql_dir=GOLD_SQL_DIR,
        contracts_dir=GOLD_CONTRACTS_DIR,
        checks_file=GOLD_CHECKS_FILE,
        output_dir=GOLD_DIR,
        observability_dir=OBSERVABILITY_DIR,
    )


def register_silver(con: duckdb.DuckDBPyConnection, name: str) -> int:
    path = SILVER_DIR / name
    if not DeltaTable.is_deltatable(str(path)):
        raise FileNotFoundError(f"Silver table '{name}' not found at {path}")

    dataset, version = read_at_version(path)
    con.register(f"{SILVER_VIEW_PREFIX}{name}", dataset)
    logger.info("Registered Silver table %s at version %d", name, version)
    return version


def business_metrics(con: duckdb.DuckDBPyConnection, run: RunContext, start: date | None, end: date | None) -> None:
    scope = "true" if start is None else f"reference_date BETWEEN '{start}' AND '{end}'"
    latest = con.sql(
        f"SELECT reference_date, discrepancy_rate, discrepancy_rate_deviation, is_comparable_volume "
        f"FROM {RECONCILIATION_TABLE} "
        f"WHERE discrepancy_rate IS NOT NULL AND {scope} ORDER BY reference_date DESC LIMIT 1"
    ).fetchone()
    if latest:
        reference_date, rate, deviation, comparable = latest
        run.metric(LAYER, RECONCILIATION_TABLE, "discrepancy_rate", rate, reference_date=reference_date)
        if deviation is not None and comparable:
            run.metric(
                LAYER, RECONCILIATION_TABLE, "discrepancy_rate_deviation", deviation, reference_date=reference_date
            )
        elif deviation is not None:
            logger.info(
                "Discrepancy rate deviation on %s not compared: too few results against the baseline",
                reference_date,
            )
        logger.info("Discrepancy rate on %s: %.4f (deviation %s)", reference_date, rate, deviation)

    missing = con.sql(f"SELECT count(*) FROM {RECONCILIATION_TABLE} WHERE is_missing_run AND {scope}").fetchone()[0]
    run.metric(LAYER, RECONCILIATION_TABLE, "missing_runs", missing, reference_date=start if start == end else None)


def build(run: RunContext, start: date | None, end: date | None) -> None:
    layer = gold_layer()
    con = connect(layer.sql_dir)
    silver_versions = {name: register_silver(con, name) for name in layer.inputs()}
    silver_rows = {name: row_count(con, f"{SILVER_VIEW_PREFIX}{name}") for name in silver_versions}
    if build_layer(con, layer, run, silver_versions, silver_rows, start, end):
        business_metrics(con, run, start, end)


def parse_args(argv: list[str] | None = None):
    return parse_date_args("Build the Gold layer (dimensional model and KPIs) from Silver", argv)


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    run_job(JOB_NAME, GOLD_DIR, OBSERVABILITY_DIR, ALERT_RULES_FILE, build, args.start, args.end)


if __name__ == "__main__":
    main()

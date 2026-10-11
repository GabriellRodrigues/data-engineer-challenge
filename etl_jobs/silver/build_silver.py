import argparse
import json
import sys
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import CommitProperties, DeltaTable

from etl_jobs.silver.tables import MACROS_FILE, SILVER_TABLES, SilverTable, bronze_inputs
from src.common.delta_io import publish, read_at_version
from src.data_contract.contract_file_validator import load_contract
from src.data_contract.schema_validator import (
    SchemaContractError,
    contract_properties,
    enforce_schema,
    undeclared_columns,
)
from src.data_quality.checks import blocking, contract_checks, load_checks, run_checks
from src.observability.alerts import evaluate_alerts, load_rules
from src.observability.metrics import table_metrics
from src.observability.run_registry import FAILED, RunContext, pipeline_run
from src.utils.logger import get_logger
from src.utils.paths import (
    ALERT_RULES_FILE,
    BRONZE_CONTRACTS_DIR,
    BRONZE_DIR,
    OBSERVABILITY_DIR,
    SILVER_CHECKS_FILE,
    SILVER_CONTRACTS_DIR,
    SILVER_DIR,
    SILVER_SQL_DIR,
)

logger = get_logger("silver.build")

JOB_NAME = "silver.build"
LAYER = "silver"
FULL_MODE = "full"
DATE_MODE = "reference_date"
BRONZE_VIEW_PREFIX = "bronze_"


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(sql_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute((sql_dir / MACROS_FILE).read_text(encoding="utf-8"))
    return con


def register_bronze(con: duckdb.DuckDBPyConnection, name: str) -> int:
    path = BRONZE_DIR / name
    if not DeltaTable.is_deltatable(str(path)):
        raise FileNotFoundError(f"Bronze table '{name}' not found at {path}")

    dataset, version = read_at_version(path)
    raw_view = f"raw_{name}"
    con.register(raw_view, dataset)

    present = set(dataset.schema.names)
    absent = [
        f"CAST(NULL AS {p['physicalType'].upper()}) AS {_identifier(p['name'])}"
        for p in contract_properties(load_contract(name, BRONZE_CONTRACTS_DIR))
        if p["name"] not in present
    ]
    columns = ", ".join(["*", *absent])
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW {BRONZE_VIEW_PREFIX}{name} AS "
        f"SELECT {columns} FROM {raw_view} "
        f"QUALIFY _ingested_at = max(_ingested_at) OVER (PARTITION BY _source_file)"
    )
    logger.info("Registered Bronze table %s at version %d", name, version)
    return version


def row_count(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    return con.sql(f"SELECT count(*) FROM {relation}").fetchone()[0]


def compute(con: duckdb.DuckDBPyConnection, table: SilverTable, run: RunContext) -> int:
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {table.name} AS "
        f"SELECT *, {_sql_string(run.run_id)} AS _silver_run_id, now() AS _silver_built_at "
        f"FROM ({table.sql(SILVER_SQL_DIR)})"
    )
    return row_count(con, table.name)


def conform(data: pa.Table, contract: dict) -> pa.Table:
    extra = undeclared_columns(data, contract)
    if extra:
        raise SchemaContractError(
            contract["name"], [f"column '{name}' is not declared in the contract" for name in extra]
        )
    data = enforce_schema(data, contract)
    return data.select([p["name"] for p in contract_properties(contract)])


def publish_table(
    con: duckdb.DuckDBPyConnection,
    table: SilverTable,
    contract: dict,
    run: RunContext,
    start: date | None,
    end: date | None,
    bronze_versions: dict[str, int],
) -> int:
    predicate = table.scope(start, end)
    where = f" WHERE {predicate}" if predicate else ""
    data = conform(con.sql(f"SELECT * FROM {table.name}{where}").to_arrow_table(), contract)

    metadata = {
        "run_id": run.run_id,
        "mode": run.mode,
        "scope": predicate or "full",
        "contract_version": contract["version"],
        "bronze_versions": json.dumps({n: bronze_versions[n] for n in table.bronze_inputs}),
    }
    publish(
        SILVER_DIR / table.name,
        data,
        predicate=predicate,
        partition_by=[table.partition_column] if table.partition_column else None,
        commit_properties=CommitProperties(custom_metadata=metadata),
    )
    logger.info(
        "Published %d rows to silver.%s (scope: %s)", data.num_rows, table.name, predicate or "full"
    )
    return data.num_rows


def quality_checks(contracts: dict[str, dict]) -> list:
    checks = [check for name, contract in contracts.items() for check in contract_checks(name, contract)]
    return checks + load_checks(SILVER_CHECKS_FILE)


def build(run: RunContext, start: date | None, end: date | None) -> None:
    con = connect(SILVER_SQL_DIR)
    bronze_versions = {name: register_bronze(con, name) for name in bronze_inputs(SILVER_TABLES)}
    bronze_rows = {name: row_count(con, f"{BRONZE_VIEW_PREFIX}{name}") for name in bronze_versions}
    published_tables = [table for table in SILVER_TABLES if table.publish]
    contracts = {table.name: load_contract(table.name, SILVER_CONTRACTS_DIR) for table in published_tables}
    reference_date = start if start and start == end else None

    for table in SILVER_TABLES:
        inputs = [f"bronze/{name}@v{bronze_versions[name]}" for name in table.bronze_inputs]
        with run.step(f"compute:{table.name}", LAYER, table.name, inputs=inputs) as step:
            step.rows_in = sum(bronze_rows[name] for name in table.bronze_inputs)
            step.rows_out = compute(con, table, run)
            if table.dedup_source:
                removed = bronze_rows[table.dedup_source] - step.rows_out
                run.metric(LAYER, table.name, "duplicates_removed", removed, reference_date=reference_date)

    with run.step("quality:silver", LAYER) as step:
        results = run_checks(con, quality_checks(contracts), run.run_id, LAYER, OBSERVABILITY_DIR)
        failed = blocking(results)
        step.rows_in = len(results)
        step.rows_rejected = len(failed)

    if failed:
        run.mark_failed(f"Blocking data quality checks failed: {', '.join(failed)}; nothing was published")
        return

    for table in published_tables:
        path = SILVER_DIR / table.name
        with run.step(f"publish:{table.name}", LAYER, table.name, path, [f"temp/{table.name}"]) as step:
            step.contract_version = contracts[table.name]["version"]
            step.rows_in = row_count(con, table.name)
            step.rows_out = publish_table(
                con, table, contracts[table.name], run, start, end, bronze_versions
            )
            run.metric(LAYER, table.name, "rows_out", step.rows_out, reference_date=reference_date)

    for table in published_tables:
        for metric, value in table_metrics(SILVER_DIR / table.name).items():
            run.metric(LAYER, table.name, metric, value)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the Silver layer from Bronze")
    parser.add_argument(
        "--reference-date",
        type=date.fromisoformat,
        help="Rebuild only this business date (YYYY-MM-DD); without dates the whole layer is rebuilt",
    )
    parser.add_argument("--start-date", type=date.fromisoformat, help="First date of a range (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=date.fromisoformat, help="Last date of a range (YYYY-MM-DD)")
    args = parser.parse_args(argv)

    if args.reference_date and (args.start_date or args.end_date):
        parser.error("--reference-date cannot be combined with --start-date/--end-date")
    if bool(args.start_date) != bool(args.end_date):
        parser.error("--start-date and --end-date must be used together")
    if args.start_date and args.start_date > args.end_date:
        parser.error("--start-date must not be after --end-date")

    args.start = args.reference_date or args.start_date
    args.end = args.reference_date or args.end_date
    return args


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    mode = DATE_MODE if args.start else FULL_MODE

    with pipeline_run(JOB_NAME, mode, OBSERVABILITY_DIR) as run:
        try:
            build(run, args.start, args.end)
        except Exception as error:
            logger.exception("Silver build failed")
            run.mark_failed(f"{type(error).__name__}: {error}")

        blocking_alerts = evaluate_alerts(run, load_rules(ALERT_RULES_FILE))
        if blocking_alerts and run.status != FAILED:
            run.mark_failed("Blocking alerts raised; see pipeline_alerts")

    if run.status == FAILED:
        logger.error("Silver build run_id=%s failed: %s", run.run_id, run.error)
        sys.exit(1)

    scope = f"{args.start} to {args.end}" if args.start else "full rebuild"
    logger.info("Silver build run_id=%s completed (%s)", run.run_id, scope)


if __name__ == "__main__":
    main()

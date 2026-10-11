import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
from deltalake import CommitProperties

from src.common.delta_io import publish
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

logger = get_logger("common.layer_build")

MACROS_FILE = "macros.sql"
FULL_MODE = "full"
DATE_MODE = "reference_date"


@dataclass(frozen=True)
class LayerTable:
    name: str
    inputs: tuple[str, ...]
    partition_column: str | None = None
    lookback_days: int = 0
    publish: bool = True
    dedup_source: str | None = None

    def sql(self, sql_dir: Path) -> str:
        return (sql_dir / f"{self.name}.sql").read_text(encoding="utf-8")

    def scope(self, start: date | None, end: date | None) -> str | None:
        if self.partition_column is None or start is None or end is None:
            return None
        first = start - timedelta(days=self.lookback_days)
        return (
            f"{self.partition_column} >= '{first.isoformat()}' "
            f"AND {self.partition_column} <= '{end.isoformat()}'"
        )


@dataclass(frozen=True)
class Layer:
    name: str
    input_layer: str
    tables: tuple[LayerTable, ...]
    sql_dir: Path
    contracts_dir: Path
    checks_file: Path
    output_dir: Path
    observability_dir: Path

    @property
    def published(self) -> list[LayerTable]:
        return [table for table in self.tables if table.publish]

    def inputs(self) -> list[str]:
        return sorted({name for table in self.tables for name in table.inputs})


def identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect(sql_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    macros = sql_dir / MACROS_FILE
    if macros.is_file():
        con.execute(macros.read_text(encoding="utf-8"))
    return con


def row_count(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    return con.sql(f"SELECT count(*) FROM {relation}").fetchone()[0]


def compute(con: duckdb.DuckDBPyConnection, layer: Layer, table: LayerTable, run: RunContext) -> int:
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE {table.name} AS "
        f"SELECT *, {sql_string(run.run_id)} AS _{layer.name}_run_id, now() AS _{layer.name}_built_at "
        f"FROM ({table.sql(layer.sql_dir)})"
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
    layer: Layer,
    table: LayerTable,
    contract: dict,
    run: RunContext,
    start: date | None,
    end: date | None,
    input_versions: dict[str, int],
) -> int:
    predicate = table.scope(start, end)
    where = f" WHERE {predicate}" if predicate else ""
    data = conform(con.sql(f"SELECT * FROM {table.name}{where}").to_arrow_table(), contract)

    metadata = {
        "run_id": run.run_id,
        "mode": run.mode,
        "scope": predicate or "full",
        "contract_version": contract["version"],
        f"{layer.input_layer}_versions": json.dumps({n: input_versions[n] for n in table.inputs}),
    }
    publish(
        layer.output_dir / table.name,
        data,
        predicate=predicate,
        partition_by=[table.partition_column] if table.partition_column else None,
        commit_properties=CommitProperties(custom_metadata=metadata),
    )
    logger.info(
        "Published %d rows to %s.%s (scope: %s)",
        data.num_rows,
        layer.name,
        table.name,
        predicate or "full",
    )
    return data.num_rows


def quality_checks(layer: Layer, contracts: dict[str, dict]) -> list:
    checks = [check for name, contract in contracts.items() for check in contract_checks(name, contract)]
    return checks + load_checks(layer.checks_file)


def build_layer(
    con: duckdb.DuckDBPyConnection,
    layer: Layer,
    run: RunContext,
    input_versions: dict[str, int],
    input_rows: dict[str, int],
    start: date | None,
    end: date | None,
) -> bool:
    contracts = {table.name: load_contract(table.name, layer.contracts_dir) for table in layer.published}
    reference_date = start if start and start == end else None

    for table in layer.tables:
        inputs = [f"{layer.input_layer}/{name}@v{input_versions[name]}" for name in table.inputs]
        with run.step(f"compute:{table.name}", layer.name, table.name, inputs=inputs) as step:
            step.rows_in = sum(input_rows[name] for name in table.inputs)
            step.rows_out = compute(con, layer, table, run)
            if table.dedup_source:
                removed = input_rows[table.dedup_source] - step.rows_out
                run.metric(layer.name, table.name, "duplicates_removed", removed, reference_date=reference_date)

    with run.step(f"quality:{layer.name}", layer.name) as step:
        results = run_checks(con, quality_checks(layer, contracts), run.run_id, layer.name, layer.observability_dir)
        failed = blocking(results)
        step.rows_in = len(results)
        step.rows_rejected = len(failed)

    if failed:
        run.mark_failed(f"Blocking data quality checks failed: {', '.join(failed)}; nothing was published")
        return False

    for table in layer.published:
        path = layer.output_dir / table.name
        with run.step(f"publish:{table.name}", layer.name, table.name, path, [f"temp/{table.name}"]) as step:
            step.contract_version = contracts[table.name]["version"]
            step.rows_in = row_count(con, table.name)
            step.rows_out = publish_table(con, layer, table, contracts[table.name], run, start, end, input_versions)
            run.metric(layer.name, table.name, "rows_out", step.rows_out, reference_date=reference_date)

    for table in layer.published:
        for metric, value in table_metrics(layer.output_dir / table.name).items():
            run.metric(layer.name, table.name, metric, value)
    return True


def parse_date_args(description: str, argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
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


def run_job(
    job_name: str,
    output_dir: Path,
    observability_dir: Path,
    alert_rules_file: Path,
    build: Callable[[RunContext, date | None, date | None], None],
    start: date | None,
    end: date | None,
) -> None:
    job_logger = get_logger(job_name)
    output_dir.mkdir(parents=True, exist_ok=True)
    mode = DATE_MODE if start else FULL_MODE

    with pipeline_run(job_name, mode, observability_dir) as run:
        try:
            build(run, start, end)
        except Exception as error:
            job_logger.exception("%s failed", job_name)
            run.mark_failed(f"{type(error).__name__}: {error}")

        blocking_alerts = evaluate_alerts(run, load_rules(alert_rules_file))
        if blocking_alerts and run.status != FAILED:
            run.mark_failed("Blocking alerts raised; see pipeline_alerts")

    if run.status == FAILED:
        job_logger.error("%s run_id=%s failed: %s", job_name, run.run_id, run.error)
        sys.exit(1)

    scope = f"{start} to {end}" if start else "full rebuild"
    job_logger.info("%s run_id=%s completed (%s)", job_name, run.run_id, scope)

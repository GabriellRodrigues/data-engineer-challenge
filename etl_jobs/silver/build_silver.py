from datetime import date

import duckdb
from deltalake import DeltaTable

from etl_jobs.silver.tables import SILVER_TABLES
from src.common.delta_io import read_at_version
from src.common.layer_build import (
    Layer,
    build_layer,
    connect,
    identifier,
    parse_date_args,
    row_count,
    run_job,
)
from src.data_contract.contract_file_validator import load_contract
from src.data_contract.schema_validator import contract_properties
from src.observability.run_registry import RunContext
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
BRONZE_VIEW_PREFIX = "bronze_"


def silver_layer() -> Layer:
    return Layer(
        name=LAYER,
        input_layer="bronze",
        tables=SILVER_TABLES,
        sql_dir=SILVER_SQL_DIR,
        contracts_dir=SILVER_CONTRACTS_DIR,
        checks_file=SILVER_CHECKS_FILE,
        output_dir=SILVER_DIR,
        observability_dir=OBSERVABILITY_DIR,
    )


def register_bronze(con: duckdb.DuckDBPyConnection, name: str) -> int:
    path = BRONZE_DIR / name
    if not DeltaTable.is_deltatable(str(path)):
        raise FileNotFoundError(f"Bronze table '{name}' not found at {path}")

    dataset, version = read_at_version(path)
    raw_view = f"raw_{name}"
    con.register(raw_view, dataset)

    present = set(dataset.schema.names)
    absent = [
        f"CAST(NULL AS {p['physicalType'].upper()}) AS {identifier(p['name'])}"
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


def build(run: RunContext, start: date | None, end: date | None) -> None:
    layer = silver_layer()
    con = connect(layer.sql_dir)
    bronze_versions = {name: register_bronze(con, name) for name in layer.inputs()}
    bronze_rows = {name: row_count(con, f"{BRONZE_VIEW_PREFIX}{name}") for name in bronze_versions}
    build_layer(con, layer, run, bronze_versions, bronze_rows, start, end)


def parse_args(argv: list[str] | None = None):
    return parse_date_args("Build the Silver layer from Bronze", argv)


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    run_job(JOB_NAME, SILVER_DIR, OBSERVABILITY_DIR, ALERT_RULES_FILE, build, args.start, args.end)


if __name__ == "__main__":
    main()

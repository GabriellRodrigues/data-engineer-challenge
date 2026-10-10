import argparse
import hashlib
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import CommitProperties, DeltaTable, write_deltalake

from src.common.delta_maintenance import TABLE_PROPERTIES
from src.data_contract.contract_file_validator import load_contract
from src.data_contract.record_validator import split_records
from src.data_contract.schema_validator import enforce_schema, undeclared_columns
from src.observability.alerts import evaluate_alerts, load_rules
from src.observability.metrics import table_metrics
from src.observability.run_registry import FAILED, SKIPPED, RunContext, pipeline_run
from src.utils.logger import get_logger
from src.utils.paths import (
    ALERT_RULES_FILE,
    BRONZE_CONTRACTS_DIR,
    BRONZE_DIR,
    OBSERVABILITY_DIR,
    SOURCE_DATA_DIR,
)

logger = get_logger("bronze.ingest")

HASH_COLUMN = "_source_file_hash"
SOURCE_FILE_COLUMN = "_source_file"
INGESTION_MODE_COLUMN = "_ingestion_mode"
INGEST_MODE = "ingest"
REPROCESS_MODE = "reprocess"
REFUSED_SUFFIX = "_refused"
JOB_NAME = "bronze.ingest"
LAYER = "bronze"
DEFAULT_EVENT_COLUMN = "_timestamp"
ARRIVAL_COLUMN = "_ingested_at"
EVENT_COLUMNS = {"settlement_paysettler": "settled_at"}
DATASET_PATTERNS = (
    (re.compile(r"(.+)_batch_\d+"), r"\1"),
    (re.compile(r"settlement_\d{4}-?\d{2}-?\d{2}.*"), "settlement_paysettler"),
)


def read_parquet(con: duckdb.DuckDBPyConnection, path: Path) -> pa.Table:
    return con.read_parquet(str(path)).to_arrow_table()


def read_csv(con: duckdb.DuckDBPyConnection, path: Path) -> pa.Table:
    return con.read_csv(
        str(path), header=True, sep=",", quotechar='"', all_varchar=True, encoding="utf-8"
    ).to_arrow_table()


READERS = {
    ".parquet": read_parquet,
    ".csv": read_csv,
}


def source_id(source_path: Path) -> str:
    return source_path.relative_to(SOURCE_DATA_DIR).as_posix()


def dataset_name(source_path: Path) -> str:
    for pattern, dataset in DATASET_PATTERNS:
        match = pattern.fullmatch(source_path.stem)
        if match:
            return match.expand(dataset)
    return source_path.stem


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ingested_hashes(table_path: Path) -> set[str]:
    if not DeltaTable.is_deltatable(str(table_path)):
        return set()

    dataset = DeltaTable(str(table_path)).to_pyarrow_dataset()
    if HASH_COLUMN not in dataset.schema.names:
        return set()

    hashes = dataset.to_table(columns=[HASH_COLUMN])[HASH_COLUMN]
    return set(pc.unique(hashes).drop_null().to_pylist())


class HashIndex:
    def __init__(self):
        self._hashes: dict[Path, set[str]] = {}

    def _table(self, table_path: Path) -> set[str]:
        if table_path not in self._hashes:
            self._hashes[table_path] = ingested_hashes(table_path)
        return self._hashes[table_path]

    def contains(self, table_path: Path, file_hash: str) -> bool:
        return file_hash in self._table(table_path)

    def add(self, table_path: Path, file_hash: str) -> None:
        self._table(table_path).add(file_hash)


@dataclass(frozen=True)
class Load:
    run_id: str
    source_file: str
    file_hash: str
    mode: str
    contract_version: str
    reason: str | None = None

    def commit_properties(self) -> CommitProperties:
        metadata = {
            "run_id": self.run_id,
            "ingestion_mode": self.mode,
            "source_file": self.source_file,
            "source_file_hash": self.file_hash,
            "contract_version": self.contract_version,
        }
        if self.reason:
            metadata["reason"] = self.reason
        return CommitProperties(custom_metadata=metadata)


def add_metadata(table: pa.Table, load: Load) -> pa.Table:
    n = table.num_rows
    ingested_at = datetime.now(UTC)
    return (
        table.append_column("_run_id", pa.array([load.run_id] * n, pa.string()))
        .append_column(SOURCE_FILE_COLUMN, pa.array([load.source_file] * n, pa.string()))
        .append_column(HASH_COLUMN, pa.array([load.file_hash] * n, pa.string()))
        .append_column(INGESTION_MODE_COLUMN, pa.array([load.mode] * n, pa.string()))
        .append_column("_ingested_at", pa.array([ingested_at] * n, pa.timestamp("us", tz="UTC")))
    )


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def delete_rows(table_path: Path, column: str, value: str, load: Load) -> int:
    if not DeltaTable.is_deltatable(str(table_path)):
        return 0
    metrics = DeltaTable(str(table_path)).delete(
        f"{column} = {_sql_string(value)}", commit_properties=load.commit_properties()
    )
    return metrics["num_deleted_rows"]


def write_table(destination_path: Path, table: pa.Table, load: Load) -> None:
    write_deltalake(
        destination_path,
        add_metadata(table, load),
        mode="append",
        schema_mode="merge",
        configuration=TABLE_PROPERTIES,
        commit_properties=load.commit_properties(),
    )


def ingest_file(
    con: duckdb.DuckDBPyConnection,
    source_path: Path,
    run: RunContext,
    hashes: HashIndex,
    reprocess: bool = False,
    reason: str | None = None,
):
    dataset = dataset_name(source_path)
    source_file = source_id(source_path)
    destination_path = BRONZE_DIR / dataset
    refused_path = BRONZE_DIR / f"{dataset}{REFUSED_SUFFIX}"

    with run.step(
        step=f"ingest:{source_file}",
        layer=LAYER,
        dataset=dataset,
        output_table=destination_path,
        inputs=[str(source_path)],
        source_file=source_file,
    ) as step:
        file_hash = file_sha256(source_path)

        if not reprocess and hashes.contains(destination_path, file_hash):
            logger.info(
                "Skipping %s: already ingested (sha256=%s)", source_file, file_hash[:12]
            )
            step.status = SKIPPED
            return

        logger.info("Ingesting %s into %s (Delta)", source_path, destination_path)

        contract = load_contract(dataset, BRONZE_CONTRACTS_DIR)
        step.contract_version = contract["version"]
        load = Load(
            run_id=run.run_id,
            source_file=source_file,
            file_hash=file_hash,
            mode=REPROCESS_MODE if reprocess else INGEST_MODE,
            contract_version=contract["version"],
            reason=reason,
        )

        df = READERS[source_path.suffix.lower()](con, source_path)
        df = enforce_schema(df, contract)
        valid, refused = split_records(con, df, contract)
        step.rows_in, step.rows_out, step.rows_rejected = df.num_rows, valid.num_rows, refused.num_rows

        if reprocess:
            removed = delete_rows(destination_path, SOURCE_FILE_COLUMN, source_file, load)
            removed_refused = delete_rows(refused_path, SOURCE_FILE_COLUMN, source_file, load)
            logger.warning(
                "Reprocessing %s (reason: %s): removed %d rows from %s and %d rows from %s",
                source_file,
                reason or "not informed",
                removed,
                destination_path,
                removed_refused,
                refused_path,
            )
        else:
            delete_rows(refused_path, HASH_COLUMN, file_hash, load)

        if refused.num_rows:
            write_table(refused_path, refused, load)
            logger.warning(
                "%d rows of %s refused by contract '%s' and sent to %s",
                refused.num_rows,
                source_file,
                contract["name"],
                refused_path,
            )

        if valid.num_rows:
            write_table(destination_path, valid, load)
            hashes.add(destination_path, file_hash)

        file_metrics = {
            "rows_read": df.num_rows,
            "rows_accepted": valid.num_rows,
            "rows_refused": refused.num_rows,
            "refused_rate": refused.num_rows / df.num_rows if df.num_rows else 0.0,
            "undeclared_columns": len(undeclared_columns(df, contract)),
        }
        for metric, value in file_metrics.items():
            run.metric(LAYER, dataset, metric, value, source_file=source_file)

        logger.info(
            "Ingestion of %s completed (%d rows read, %d accepted, %d refused)",
            source_file,
            df.num_rows,
            valid.num_rows,
            refused.num_rows,
        )


def expected_datasets(sources: list[Path]) -> list[str]:
    contracted = {path.stem for path in BRONZE_CONTRACTS_DIR.glob("*.yml")}
    return sorted(contracted | {dataset_name(path) for path in sources})


def record_table_metrics(run: RunContext, datasets: list[str]) -> None:
    for dataset in datasets:
        tables = (
            (dataset, EVENT_COLUMNS.get(dataset, DEFAULT_EVENT_COLUMN), ARRIVAL_COLUMN),
            (f"{dataset}{REFUSED_SUFFIX}", None, None),
        )
        for table_name, event_column, arrival_column in tables:
            path = BRONZE_DIR / table_name
            if not DeltaTable.is_deltatable(str(path)):
                continue
            for metric, value in table_metrics(path, event_column, arrival_column).items():
                run.metric(LAYER, table_name, metric, value)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest source files into the Bronze layer")
    parser.add_argument(
        "--reprocess",
        nargs="+",
        metavar="FILE",
        help="Re-ingest only these source files (path relative to DATA_PATH), replacing their rows in Bronze",
    )
    parser.add_argument(
        "--reason",
        help="Why the files are reprocessed; stored in the Delta commit metadata",
    )
    args = parser.parse_args(argv)
    if args.reason and not args.reprocess:
        parser.error("--reason can only be used with --reprocess")
    return args


def select_sources(reprocess: list[str] | None) -> list[Path]:
    sources = sorted(
        path for path in SOURCE_DATA_DIR.rglob("*")
        if path.is_file() and path.suffix.lower() in READERS
    )
    if not reprocess:
        return sources

    by_id = {source_id(path): path for path in sources}
    unknown = sorted(set(reprocess) - by_id.keys())
    if unknown:
        raise ValueError(f"Files to reprocess not found in {SOURCE_DATA_DIR}: {', '.join(unknown)}")
    return [by_id[name] for name in sorted(set(reprocess))]


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    try:
        sources = select_sources(args.reprocess)
    except ValueError as error:
        logger.error("%s", error)
        sys.exit(1)
    reprocess = bool(args.reprocess)

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    mode = REPROCESS_MODE if reprocess else INGEST_MODE

    con = duckdb.connect()
    hashes = HashIndex()
    with pipeline_run(JOB_NAME, mode, OBSERVABILITY_DIR) as run:
        failed = []
        for source_path in sources:
            try:
                ingest_file(con, source_path, run, hashes, reprocess, args.reason)
            except Exception:
                logger.exception("Ingestion of %s failed", source_id(source_path))
                failed.append(source_id(source_path))

        record_table_metrics(run, expected_datasets(sources))
        blocking_alerts = evaluate_alerts(run, load_rules(ALERT_RULES_FILE))

        reasons = []
        if failed:
            reasons.append(f"Ingestion failed for: {', '.join(failed)}")
        if blocking_alerts:
            reasons.append("Blocking alerts raised; see pipeline_alerts")
        if reasons:
            run.mark_failed("; ".join(reasons))

    if run.status == FAILED:
        logger.error("Bronze ingestion run_id=%s failed: %s", run.run_id, run.error)
        sys.exit(1)

    logger.info("Bronze ingestion run_id=%s completed", run.run_id)


if __name__ == "__main__":
    main()

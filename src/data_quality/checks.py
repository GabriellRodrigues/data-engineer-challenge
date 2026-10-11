from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from src.data_contract.schema_validator import contract_properties
from src.observability.store import DQ_RESULTS, append
from src.utils.logger import get_logger
from src.utils.yaml_reader import read_yaml

logger = get_logger("data_quality.checks")

ERROR = "error"
WARN = "warn"
SEVERITIES = {ERROR, WARN}
PASS = "pass"
FAIL = "fail"
BREACH_STATUS = {WARN: WARN, ERROR: FAIL}


@dataclass(frozen=True)
class Check:
    name: str
    table: str
    severity: str
    query: str
    type: str = "sql"
    threshold: int = 0
    description: str | None = None


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def unique_check(table: str, columns: list[str], severity: str = ERROR) -> Check:
    keys = ", ".join(_identifier(c) for c in columns)
    return Check(
        name=f"unique:{table}:{'+'.join(columns)}",
        table=table,
        severity=severity,
        type="unique",
        query=(
            f"SELECT coalesce(sum(n - 1), 0) FROM "
            f"(SELECT count(*) AS n FROM {_identifier(table)} GROUP BY {keys} HAVING count(*) > 1)"
        ),
    )


def not_null_check(table: str, column: str, severity: str = ERROR) -> Check:
    return Check(
        name=f"not_null:{table}:{column}",
        table=table,
        severity=severity,
        type="not_null",
        query=f"SELECT count(*) FROM {_identifier(table)} WHERE {_identifier(column)} IS NULL",
    )


def contract_checks(table: str, contract: dict) -> list[Check]:
    properties = contract_properties(contract)
    checks = [not_null_check(table, p["name"]) for p in properties if p["required"]]
    keys = sorted(
        (p for p in properties if p.get("primaryKey")),
        key=lambda p: p.get("primaryKeyPosition", 0),
    )
    if keys:
        checks.append(unique_check(table, [p["name"] for p in keys]))
    return checks


def load_checks(path: Path) -> list[Check]:
    checks = [Check(**raw) for raw in read_yaml(path).get("checks") or []]
    for check in checks:
        if check.severity not in SEVERITIES:
            raise ValueError(f"Check '{check.name}': severity must be one of {sorted(SEVERITIES)}")
    return checks


def evaluate(con: duckdb.DuckDBPyConnection, check: Check) -> tuple[str, int | None, str]:
    try:
        failing_rows = int(con.sql(check.query).fetchone()[0] or 0)
    except duckdb.Error as error:
        return FAIL, None, f"check query failed: {error}"
    message = f"{failing_rows} failing rows (threshold {check.threshold})"
    status = PASS if failing_rows <= check.threshold else BREACH_STATUS[check.severity]
    return status, failing_rows, message


def run_checks(
    con: duckdb.DuckDBPyConnection,
    checks: list[Check],
    run_id: str,
    layer: str,
    base_dir: Path,
) -> list[dict]:
    results = []
    for check in checks:
        status, failing_rows, message = evaluate(con, check)
        results.append({
            "run_id": run_id,
            "check_name": check.name,
            "check_type": check.type,
            "layer": layer,
            "table_name": check.table,
            "severity": check.severity,
            "status": status,
            "failing_rows": failing_rows,
            "threshold": check.threshold,
            "message": message,
            "evaluated_at": datetime.now(UTC),
        })
        if status == FAIL:
            logger.error("Check %s failed on %s: %s", check.name, check.table, message)
        elif status == WARN:
            logger.warning("Check %s warned on %s: %s", check.name, check.table, message)

    append(base_dir, DQ_RESULTS, results)
    passed = sum(1 for r in results if r["status"] == PASS)
    logger.info("Data quality: %d of %d checks passed on layer %s", passed, len(results), layer)
    return results


def blocking(results: list[dict]) -> list[str]:
    return [r["check_name"] for r in results if r["status"] == FAIL]

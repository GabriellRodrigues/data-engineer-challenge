from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from src.utils.paths import SILVER_SQL_DIR

RECONCILIATION_WINDOW_DAYS = 7
MACROS_FILE = "macros.sql"


@dataclass(frozen=True)
class SilverTable:
    name: str
    bronze_inputs: tuple[str, ...]
    partition_column: str | None = None
    lookback_days: int = 0
    publish: bool = True
    dedup_source: str | None = None

    def sql(self, sql_dir: Path = SILVER_SQL_DIR) -> str:
        return (sql_dir / f"{self.name}.sql").read_text(encoding="utf-8")

    def scope(self, start: date | None, end: date | None) -> str | None:
        if self.partition_column is None or start is None or end is None:
            return None
        first = start - timedelta(days=self.lookback_days)
        return (
            f"{self.partition_column} >= '{first.isoformat()}' "
            f"AND {self.partition_column} <= '{end.isoformat()}'"
        )


SILVER_TABLES = (
    SilverTable(
        "transactions_history",
        ("transactions",),
        "created_date",
        RECONCILIATION_WINDOW_DAYS,
        dedup_source="transactions",
    ),
    SilverTable("transactions", ("transactions",), "created_date", RECONCILIATION_WINDOW_DAYS),
    SilverTable(
        "settlements",
        ("settlement_paysettler",),
        "reference_date",
        dedup_source="settlement_paysettler",
    ),
    SilverTable("reconciliation_runs", ("reconciliation_runs",), "reference_date"),
    SilverTable(
        "reconciliation_results_deduped",
        ("reconciliation_results",),
        publish=False,
        dedup_source="reconciliation_results",
    ),
    SilverTable(
        "reconciliation_results",
        ("reconciliation_results", "reconciliation_runs"),
        "reference_date",
    ),
    SilverTable(
        "reconciliation_results_quarantine",
        ("reconciliation_results", "reconciliation_runs"),
    ),
    SilverTable("merchants", ("enterprise_company",)),
)


def bronze_inputs(tables: tuple[SilverTable, ...] = SILVER_TABLES) -> list[str]:
    return sorted({name for table in tables for name in table.bronze_inputs})

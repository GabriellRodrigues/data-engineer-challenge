from pathlib import Path

from deltalake import DeltaTable

from src.utils.logger import get_logger

logger = get_logger("common.delta_maintenance")

RETENTION_DAYS = 30

OPTIMIZE_OPERATION = "OPTIMIZE"
MAINTENANCE_OPERATIONS = {
    "SET TBLPROPERTIES",
    "VACUUM START",
    "VACUUM END",
}

TABLE_PROPERTIES = {
    "delta.deletedFileRetentionDuration": f"interval {RETENTION_DAYS} days",
    "delta.logRetentionDuration": f"interval {RETENTION_DAYS} days",
}


def apply_table_properties(table: DeltaTable) -> None:
    current = table.metadata().configuration
    pending = {k: v for k, v in TABLE_PROPERTIES.items() if current.get(k) != v}
    if pending:
        table.alter.set_table_properties(pending)


def needs_optimize(table: DeltaTable) -> bool:
    for commit in table.history():
        operation = commit["operation"]
        if operation == OPTIMIZE_OPERATION:
            return False
        if operation not in MAINTENANCE_OPERATIONS:
            return True
    return False


def optimize_table(table: DeltaTable, zorder_columns: list[str] | None) -> dict:
    if zorder_columns:
        return table.optimize.z_order(zorder_columns)
    return table.optimize.compact()


def maintain_table(path: Path, zorder_columns: list[str] | None = None) -> dict:
    table = DeltaTable(str(path))
    apply_table_properties(table)

    if needs_optimize(table):
        optimize_metrics = optimize_table(table, zorder_columns)
    else:
        logger.info("Skipping optimize of %s: no data changes since the last OPTIMIZE", path.name)
        optimize_metrics = {"numFilesAdded": 0, "numFilesRemoved": 0}

    vacuumed_files = table.vacuum(dry_run=False)

    table.create_checkpoint()
    table.cleanup_metadata()

    return {
        "version": table.version(),
        "files_added": optimize_metrics["numFilesAdded"],
        "files_removed": optimize_metrics["numFilesRemoved"],
        "files_vacuumed": len(vacuumed_files),
    }

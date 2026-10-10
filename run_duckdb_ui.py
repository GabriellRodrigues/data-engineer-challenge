import os
import threading
from pathlib import Path

import duckdb
from deltalake import DeltaTable

from src.utils.logger import get_logger
from src.utils.paths import BRONZE_DIR, GOLD_DIR, OBSERVABILITY_DIR, SILVER_DIR

logger = get_logger("duckdb.ui")

LAYERS = {
    "bronze": BRONZE_DIR,
    "silver": SILVER_DIR,
    "gold": GOLD_DIR,
    "observability": OBSERVABILITY_DIR,
}
UI_PORT = int(os.getenv("DUCKDB_UI_PORT", "4213"))


def delta_tables(layer_dir: Path) -> list[Path]:
    if not layer_dir.is_dir():
        return []
    return sorted(p for p in layer_dir.iterdir() if p.is_dir() and DeltaTable.is_deltatable(str(p)))


def register_views(con: duckdb.DuckDBPyConnection) -> None:
    for layer, layer_dir in LAYERS.items():
        con.execute(f'CREATE SCHEMA IF NOT EXISTS "{layer}"')
        for table_path in delta_tables(layer_dir):
            con.execute(
                f'CREATE OR REPLACE VIEW "{layer}"."{table_path.name}" AS '
                f"SELECT * FROM delta_scan('{table_path.as_posix()}')"
            )
            logger.info("Registered view %s.%s", layer, table_path.name)


def main():
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta; INSTALL ui; LOAD ui")
    con.execute(f"SET ui_local_port = {UI_PORT}")

    register_views(con)

    con.execute("CALL start_ui()")
    logger.info("DuckDB UI running at http://localhost:%d (Ctrl+C to stop)", UI_PORT)

    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        con.execute("CALL stop_ui_server()")
        logger.info("DuckDB UI stopped")


if __name__ == "__main__":
    main()

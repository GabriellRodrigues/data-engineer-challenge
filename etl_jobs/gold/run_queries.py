import argparse
import sys
from pathlib import Path

import duckdb
from deltalake import DeltaTable

from src.utils.logger import get_logger
from src.utils.paths import GOLD_DIR, GOLD_QUERIES_DIR

logger = get_logger("gold.queries")

CONSUMERS = ("operations", "cfo", "compliance")
DEFAULT_MAX_ROWS = 20


def register_gold(con: duckdb.DuckDBPyConnection, gold_dir: Path) -> list[str]:
    if not gold_dir.is_dir():
        raise FileNotFoundError(f"Gold layer not found at {gold_dir}; run the Gold build first")
    tables = sorted(p.name for p in gold_dir.iterdir() if p.is_dir() and DeltaTable.is_deltatable(str(p)))
    for name in tables:
        con.register(name, DeltaTable(str(gold_dir / name)).to_pyarrow_dataset())
    return tables


def discover(queries_dir: Path, consumer: str | None = None) -> list[Path]:
    consumers = [consumer] if consumer else list(CONSUMERS)
    return [path for name in consumers for path in sorted((queries_dir / name).glob("*.sql"))]


def run_query(con: duckdb.DuckDBPyConnection, path: Path) -> tuple[list[str], list[tuple]]:
    relation = con.sql(path.read_text(encoding="utf-8"))
    return relation.columns, relation.fetchall()


def _cell(value) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value).replace("|", "\\|").replace("\n", " ")


def to_markdown(columns: list[str], rows: list[tuple], max_rows: int = DEFAULT_MAX_ROWS) -> str:
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(_cell(v) for v in row) + " |" for row in rows[:max_rows]]
    if len(rows) > max_rows:
        lines.append(f"\n_{len(rows) - max_rows} more rows not shown_")
    return "\n".join(lines)


def run_all(
    queries: list[Path], gold_dir: Path, max_rows: int = DEFAULT_MAX_ROWS
) -> tuple[list[str], list[str]]:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    register_gold(con, gold_dir)

    sections, failed = [], []
    for path in queries:
        name = f"{path.parent.name}/{path.stem}"
        try:
            columns, rows = run_query(con, path)
        except duckdb.Error as error:
            logger.error("Query %s failed: %s", name, error)
            failed.append(name)
            continue
        table = to_markdown(columns, rows, max_rows)
        logger.info("Query %s returned %d rows\n%s", name, len(rows), table)
        sections.append(f"## {name}\n\n{table}\n")
    return sections, failed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the example analytical queries against the Gold layer")
    parser.add_argument("--consumer", choices=CONSUMERS, help="Run only the queries of one consumer")
    parser.add_argument("--output", type=Path, help="Also write the results to this Markdown file")
    parser.add_argument("--max-rows", type=int, default=DEFAULT_MAX_ROWS, help="Rows shown per query")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    queries = discover(GOLD_QUERIES_DIR, args.consumer)
    sections, failed = run_all(queries, GOLD_DIR, args.max_rows)

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text("# Gold example queries\n\n" + "\n".join(sections), encoding="utf-8")
        logger.info("Query results written to %s", args.output)

    if failed:
        logger.error("%d of %d queries failed: %s", len(failed), len(queries), ", ".join(failed))
        sys.exit(1)
    logger.info("%d queries ran successfully", len(queries))


if __name__ == "__main__":
    main()

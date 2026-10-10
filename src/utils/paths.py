import os
from pathlib import Path

_ROOT_MARKER = "pyproject.toml"


def _find_project_root() -> Path:
    env_root = os.getenv("PROJECT_ROOT")
    if env_root:
        return Path(env_root).resolve()

    for parent in Path(__file__).resolve().parents:
        if (parent / _ROOT_MARKER).is_file():
            return parent

    raise RuntimeError(f"Project root not found (no {_ROOT_MARKER} above {__file__})")


PROJECT_ROOT = _find_project_root()


def project_path(*parts: str | os.PathLike) -> Path:
    return PROJECT_ROOT.joinpath(*parts)


CONTRACTS_DIR = project_path("contracts")

DATA_DIR = project_path("data")
SOURCE_DATA_DIR = Path(os.getenv("DATA_PATH", DATA_DIR / "landing"))
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
OBSERVABILITY_DIR = DATA_DIR / "observability"
OBSERVABILITY_REPORTS_DIR = OBSERVABILITY_DIR / "reports"

ETLS_DIR = project_path("etl_jobs")
BRONZE_ETL_DIR = ETLS_DIR / "bronze"
SILVER_ETL_DIR = ETLS_DIR / "silver"
GOLD_ETL_DIR = ETLS_DIR / "gold"
OBSERVABILITY_ETL_DIR = ETLS_DIR / "observability"

BRONZE_CONTRACTS_DIR = BRONZE_ETL_DIR / "contracts"
ALERT_RULES_FILE = OBSERVABILITY_ETL_DIR / "alert_rules.yml"

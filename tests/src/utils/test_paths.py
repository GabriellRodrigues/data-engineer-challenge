from pathlib import Path

from src.utils import paths


def test_project_root_is_the_folder_with_pyproject():
    assert (paths.PROJECT_ROOT / "pyproject.toml").is_file()


def test_project_root_can_be_overridden(tmp_path, monkeypatch):
    monkeypatch.setenv("PROJECT_ROOT", str(tmp_path))

    assert paths._find_project_root() == tmp_path.resolve()


def test_project_path_joins_parts():
    assert paths.project_path("data", "bronze") == paths.PROJECT_ROOT / "data" / "bronze"


def test_layer_and_job_folders_live_under_the_project_root():
    assert paths.BRONZE_DIR == paths.PROJECT_ROOT / "data" / "bronze"
    assert paths.OBSERVABILITY_DIR == paths.PROJECT_ROOT / "data" / "observability"
    assert paths.BRONZE_CONTRACTS_DIR == paths.PROJECT_ROOT / "etl_jobs" / "bronze" / "contracts"
    assert paths.ALERT_RULES_FILE.is_file()


def test_source_data_dir_is_a_path():
    assert isinstance(paths.SOURCE_DATA_DIR, Path)

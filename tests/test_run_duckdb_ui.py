import pyarrow as pa
from deltalake import write_deltalake

import run_duckdb_ui


def test_delta_tables_lists_only_delta_folders(tmp_path):
    write_deltalake(tmp_path / "orders", pa.table({"id": [1]}))
    write_deltalake(tmp_path / "customers", pa.table({"id": [1]}))
    (tmp_path / "not_delta").mkdir()
    (tmp_path / "file.txt").write_text("x", encoding="utf-8")

    assert [p.name for p in run_duckdb_ui.delta_tables(tmp_path)] == ["customers", "orders"]


def test_delta_tables_of_missing_folder_is_empty(tmp_path):
    assert run_duckdb_ui.delta_tables(tmp_path / "missing") == []


def test_every_layer_is_exposed():
    assert set(run_duckdb_ui.LAYERS) == {"bronze", "silver", "gold", "observability"}

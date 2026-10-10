import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from src.common.delta_maintenance import maintain_table, needs_optimize


def operations(path) -> list[str]:
    return [commit["operation"] for commit in DeltaTable(str(path)).history()]


def append(path, values: list[int]) -> None:
    write_deltalake(path, pa.table({"id": values}), mode="append")


def test_new_table_needs_optimize(tmp_path):
    append(tmp_path, [1])
    assert needs_optimize(DeltaTable(str(tmp_path)))


def test_zorder_is_not_repeated_without_new_data(tmp_path):
    append(tmp_path, [1])
    append(tmp_path, [2])

    first = maintain_table(tmp_path, ["id"])
    second = maintain_table(tmp_path, ["id"])

    assert first["files_added"] == 1
    assert second["files_added"] == 0
    assert second["version"] == first["version"]
    assert operations(tmp_path).count("OPTIMIZE") == 1


def test_new_write_after_optimize_triggers_it_again(tmp_path):
    append(tmp_path, [1])
    maintain_table(tmp_path, ["id"])

    append(tmp_path, [2])
    metrics = maintain_table(tmp_path, ["id"])

    assert metrics["files_added"] == 1
    assert operations(tmp_path).count("OPTIMIZE") == 2


def test_delete_after_optimize_triggers_it_again(tmp_path):
    append(tmp_path, [1, 2])
    maintain_table(tmp_path, ["id"])

    DeltaTable(str(tmp_path)).delete("id = 2")

    assert needs_optimize(DeltaTable(str(tmp_path)))


def test_maintenance_commits_do_not_trigger_optimize(tmp_path):
    append(tmp_path, [1])
    maintain_table(tmp_path, ["id"])

    DeltaTable(str(tmp_path)).alter.set_table_properties({"delta.appendOnly": "false"})

    assert not needs_optimize(DeltaTable(str(tmp_path)))

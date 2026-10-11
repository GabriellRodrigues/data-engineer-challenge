from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
from deltalake import CommitProperties, DeltaTable, write_deltalake

from src.common.delta_maintenance import TABLE_PROPERTIES


def read_at_version(path: Path, version: int | None = None) -> tuple[ds.Dataset, int]:
    table = DeltaTable(str(path), version=version)
    return table.to_pyarrow_dataset(), table.version()


def publish(
    path: Path,
    data: pa.Table,
    predicate: str | None = None,
    partition_by: list[str] | None = None,
    commit_properties: CommitProperties | None = None,
) -> None:
    write_deltalake(
        path,
        data,
        mode="overwrite",
        predicate=predicate,
        partition_by=partition_by,
        schema_mode=None if predicate else "overwrite",
        configuration=TABLE_PROPERTIES,
        commit_properties=commit_properties,
    )

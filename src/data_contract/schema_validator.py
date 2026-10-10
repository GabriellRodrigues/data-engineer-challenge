import re

import pyarrow as pa

from src.utils.logger import get_logger

logger = get_logger("data_contract.schema")

PHYSICAL_TYPES = {
    "bigint": pa.int64(),
    "integer": pa.int32(),
    "smallint": pa.int16(),
    "double": pa.float64(),
    "float": pa.float32(),
    "varchar": pa.string(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
}


class SchemaContractError(ValueError):
    def __init__(self, contract_name: str, errors: list[str]):
        self.contract_name = contract_name
        self.errors = errors
        details = "\n".join(f"  - {e}" for e in errors)
        super().__init__(f"Schema does not match data contract '{contract_name}':\n{details}")


def contract_properties(contract: dict) -> list[dict]:
    objects = contract["schema"]
    for obj in objects:
        if obj["name"] == contract["name"]:
            return obj["properties"]
    if len(objects) == 1:
        return objects[0]["properties"]
    raise ValueError(f"Contract '{contract['name']}' has no schema object named after it")


def _normalize(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _type_matches(actual: pa.DataType, expected: pa.DataType) -> bool:
    if pa.types.is_string(expected):
        return pa.types.is_string(actual) or pa.types.is_large_string(actual)
    return actual == expected


def undeclared_columns(table: pa.Table, contract: dict) -> list[str]:
    declared = {p["name"] for p in contract_properties(contract)}
    return [name for name in table.column_names if name not in declared]


def enforce_schema(table: pa.Table, contract: dict) -> pa.Table:
    contract_name = contract["name"]
    expected = {p["name"]: p for p in contract_properties(contract)}
    actual = set(table.column_names)

    missing = [name for name in expected if name not in actual]
    extra = undeclared_columns(table, contract)
    extra_by_normalized = {_normalize(name): name for name in extra}

    errors = []
    for name in missing:
        candidate = extra_by_normalized.get(_normalize(name))
        if candidate:
            errors.append(f"column '{name}' was renamed to '{candidate}'")
        elif expected[name]["required"]:
            errors.append(f"required column '{name}' was removed")

    for name, prop in expected.items():
        if name not in actual:
            continue
        actual_type = table.schema.field(name).type
        expected_type = PHYSICAL_TYPES[prop["physicalType"]]
        if not _type_matches(actual_type, expected_type):
            errors.append(
                f"column '{name}' has type '{actual_type}' but the contract expects "
                f"'{prop['physicalType']}'"
            )

    if errors:
        raise SchemaContractError(contract_name, errors)

    for name in extra:
        logger.warning(
            "Column '%s' is not declared in contract '%s'; it will be added to the table",
            name,
            contract_name,
        )

    for name in missing:
        logger.warning(
            "Optional column '%s' of contract '%s' is absent; filling it with nulls",
            name,
            contract_name,
        )
        expected_type = PHYSICAL_TYPES[expected[name]["physicalType"]]
        table = table.append_column(pa.field(name, expected_type), pa.nulls(table.num_rows, expected_type))

    return table

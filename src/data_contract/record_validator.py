import duckdb
import pyarrow as pa
import pyarrow.compute as pc

from src.data_contract.schema_validator import contract_properties
from src.utils.logger import get_logger

logger = get_logger("data_contract.records")

REFUSED_REASONS_COLUMN = "_refused_reasons"
STRING_PHYSICAL_TYPES = {"varchar"}


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _literal(value) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return repr(value)


def _property_rules(prop: dict) -> list[tuple[str, str]]:
    name = prop["name"]
    column = _identifier(name)
    rules = []

    if prop["required"]:
        rules.append((f"{column} IS NULL", f"{name}: required value is null"))

    if prop["logicalType"] == "date" and prop["physicalType"] in STRING_PHYSICAL_TYPES:
        rules.append(
            (f"TRY_CAST({column} AS TIMESTAMPTZ) IS NULL AND {column} IS NOT NULL",
             f"{name}: value is not a valid ISO 8601 date/timestamp")
        )

    options = prop.get("logicalTypeOptions") or {}
    if "pattern" in options:
        rules.append(
            (f"NOT regexp_full_match({column}, {_literal(options['pattern'])})",
             f"{name}: value does not match pattern {options['pattern']}")
        )
    if "minimum" in options:
        rules.append((f"{column} < {_literal(options['minimum'])}", f"{name}: value below minimum {options['minimum']}"))
    if "maximum" in options:
        rules.append((f"{column} > {_literal(options['maximum'])}", f"{name}: value above maximum {options['maximum']}"))
    if "minLength" in options:
        rules.append((f"length({column}) < {options['minLength']}", f"{name}: shorter than {options['minLength']}"))
    if "maxLength" in options:
        rules.append((f"length({column}) > {options['maxLength']}", f"{name}: longer than {options['maxLength']}"))

    for quality in prop.get("quality") or []:
        if quality.get("rule") == "validValues":
            values = ", ".join(_literal(v) for v in quality["validValues"])
            rules.append((f"{column} NOT IN ({values})", f"{name}: value not in {quality['validValues']}"))

    return rules


def _validation_query(source: str, contract: dict) -> str:
    rules = [rule for prop in contract_properties(contract) for rule in _property_rules(prop)]
    if not rules:
        return f"SELECT *, []::VARCHAR[] AS {REFUSED_REASONS_COLUMN} FROM {source}"

    checks = ",\n        ".join(f"CASE WHEN {condition} THEN {_literal(message)} END" for condition, message in rules)
    return (
        f"SELECT *, list_filter([\n        {checks}\n    ], r -> r IS NOT NULL) AS {REFUSED_REASONS_COLUMN}\n"
        f"FROM {source}"
    )


def split_records(
    con: duckdb.DuckDBPyConnection, table: pa.Table, contract: dict
) -> tuple[pa.Table, pa.Table]:
    source = "_contract_source"
    con.register(source, table)
    try:
        checked = con.sql(_validation_query(source, contract)).to_arrow_table()
    finally:
        con.unregister(source)

    is_valid = pc.equal(pc.list_value_length(checked[REFUSED_REASONS_COLUMN]), 0)
    valid = checked.filter(is_valid).drop_columns([REFUSED_REASONS_COLUMN])
    refused = checked.filter(pc.invert(is_valid))

    if refused.num_rows:
        reasons = pc.value_counts(pc.list_flatten(refused[REFUSED_REASONS_COLUMN]))
        for item in reasons.to_pylist():
            logger.warning(
                "Contract '%s' violation: %s (%d rows)", contract["name"], item["values"], item["counts"]
            )

    return valid, refused

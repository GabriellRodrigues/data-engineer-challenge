import pyarrow as pa
import pytest

from src.data_contract.schema_validator import (
    SchemaContractError,
    contract_properties,
    enforce_schema,
    undeclared_columns,
)


def test_matching_schema_passes(contract, orders):
    table = orders()
    assert enforce_schema(table, contract).equals(table)


def test_removed_required_column_breaks(contract, orders):
    with pytest.raises(SchemaContractError, match="required column 'amount' was removed"):
        enforce_schema(orders(amount=None), contract)


def test_renamed_column_breaks(contract, orders):
    table = orders(status=None, Status=pa.array(["OK", "KO"]))
    with pytest.raises(SchemaContractError, match="'status' was renamed to 'Status'"):
        enforce_schema(table, contract)


def test_renamed_optional_column_breaks(contract, orders):
    table = orders(note=None, NOTE=pa.array(["a", "b"]))
    with pytest.raises(SchemaContractError, match="'note' was renamed to 'NOTE'"):
        enforce_schema(table, contract)


def test_type_change_breaks(contract, orders):
    with pytest.raises(SchemaContractError, match="column 'amount' has type 'string'"):
        enforce_schema(orders(amount=pa.array(["10", "20"])), contract)


def test_new_column_is_kept_with_warning(contract, orders, caplog):
    table = enforce_schema(orders(channel=pa.array(["web", "app"])), contract)
    assert "channel" in table.column_names
    assert "Column 'channel' is not declared in contract 'orders'" in caplog.text


def test_missing_optional_column_is_filled_with_nulls(contract, orders):
    table = enforce_schema(orders(note=None), contract)
    assert table["note"].null_count == table.num_rows


def test_undeclared_columns_lists_columns_outside_the_contract(contract, orders):
    table = orders(channel=pa.array(["web", "app"]), origin=pa.array(["a", "b"]))
    assert undeclared_columns(table, contract) == ["channel", "origin"]


def test_contract_properties_picks_the_object_named_after_the_contract(contract):
    other = {"name": "other", "properties": [{"name": "x"}]}
    multi = {**contract, "schema": [other, *contract["schema"]]}

    assert [p["name"] for p in contract_properties(multi)][:2] == ["order_id", "amount"]


def test_contract_properties_without_matching_object_fails(contract):
    multi = {**contract, "schema": [{"name": "a", "properties": []}, {"name": "b", "properties": []}]}
    with pytest.raises(ValueError, match="no schema object named after it"):
        contract_properties(multi)

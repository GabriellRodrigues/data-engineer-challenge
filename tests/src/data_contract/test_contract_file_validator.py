import pytest

from src.data_contract.contract_file_validator import (
    ContractValidationError,
    load_contract,
    validate_contract,
)
from src.utils.paths import BRONZE_CONTRACTS_DIR, GOLD_CONTRACTS_DIR, SILVER_CONTRACTS_DIR

BRONZE_CONTRACTS = sorted(path.stem for path in BRONZE_CONTRACTS_DIR.glob("*.yml"))
SILVER_CONTRACTS = sorted(path.stem for path in SILVER_CONTRACTS_DIR.glob("*.yml"))
GOLD_CONTRACTS = sorted(path.stem for path in GOLD_CONTRACTS_DIR.glob("*.yml"))


@pytest.mark.parametrize("name", BRONZE_CONTRACTS)
def test_bronze_contracts_are_valid(name):
    assert load_contract(name, BRONZE_CONTRACTS_DIR)["name"] == name


@pytest.mark.parametrize("name", SILVER_CONTRACTS)
def test_silver_contracts_are_valid(name):
    assert load_contract(name, SILVER_CONTRACTS_DIR)["name"] == name


@pytest.mark.parametrize("name", GOLD_CONTRACTS)
def test_gold_contracts_are_valid(name):
    assert load_contract(name, GOLD_CONTRACTS_DIR)["name"] == name


def test_unsupported_physical_type_is_rejected(tmp_path):
    (tmp_path / "bad.yml").write_text(
        "apiVersion: v3.0.2\n"
        "kind: DataContract\n"
        "id: test.bad\n"
        "name: bad\n"
        "version: 1.0.0\n"
        "status: active\n"
        "schema:\n"
        "  - name: bad\n"
        "    properties:\n"
        "      - {name: amount, logicalType: number, physicalType: money, required: true}\n",
        encoding="utf-8",
    )
    with pytest.raises(ContractValidationError, match="unsupported physicalType 'money'"):
        load_contract("bad", tmp_path)


def contract(**overrides) -> dict:
    base = {
        "apiVersion": "v3.0.2",
        "kind": "DataContract",
        "id": "test.orders",
        "name": "orders",
        "version": "1.0.0",
        "status": "active",
        "schema": [{
            "name": "orders",
            "properties": [
                {"name": "id", "logicalType": "integer", "physicalType": "bigint", "required": True},
            ],
        }],
    }
    base.update(overrides)
    return base


def errors_of(raw: dict) -> list[str]:
    with pytest.raises(ContractValidationError) as error:
        validate_contract(raw)
    return error.value.errors


def test_valid_contract_is_returned_unchanged():
    raw = contract()
    assert validate_contract(raw) is raw


def test_missing_top_level_field():
    assert "missing required field 'version'" in errors_of(contract(version=None))


def test_wrong_kind():
    assert "'kind' must be 'DataContract', got 'Table'" in errors_of(contract(kind="Table"))


def test_schema_must_be_a_list():
    assert "'schema' must be a list" in errors_of(contract(schema={"name": "orders"}))


def test_property_errors_are_reported_with_location():
    props = [
        {"name": "id", "logicalType": "money", "physicalType": "bigint", "required": "yes"},
        {"name": "id", "logicalType": "integer", "physicalType": "bigint", "required": True},
        {"name": "amount", "physicalType": "double", "required": True},
    ]
    errors = errors_of(contract(schema=[{"name": "orders", "properties": props}]))

    assert "schema.orders.id: invalid logicalType 'money'" in errors
    assert "schema.orders.id: 'required' must be a boolean" in errors
    assert "schema.orders.id: duplicated property name" in errors
    assert "schema.orders.amount: missing required field 'logicalType'" in errors


def test_load_missing_contract_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_contract("missing", tmp_path)

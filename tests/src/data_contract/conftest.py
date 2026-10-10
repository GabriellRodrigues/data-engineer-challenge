import pyarrow as pa
import pytest

from src.data_contract.contract_file_validator import validate_contract


@pytest.fixture
def contract() -> dict:
    return validate_contract(
        {
            "apiVersion": "v3.0.2",
            "kind": "DataContract",
            "id": "test.orders",
            "name": "orders",
            "version": "1.0.0",
            "status": "active",
            "schema": [
                {
                    "name": "orders",
                    "properties": [
                        {"name": "order_id", "logicalType": "string", "physicalType": "varchar",
                         "required": True, "logicalTypeOptions": {"pattern": "O-[0-9]+"}},
                        {"name": "amount", "logicalType": "number", "physicalType": "double",
                         "required": True, "logicalTypeOptions": {"minimum": 0}},
                        {"name": "status", "logicalType": "string", "physicalType": "varchar",
                         "required": True,
                         "quality": [{"type": "library", "rule": "validValues", "validValues": ["OK", "KO"]}]},
                        {"name": "created_at", "logicalType": "date", "physicalType": "varchar",
                         "required": True},
                        {"name": "note", "logicalType": "string", "physicalType": "varchar",
                         "required": False},
                    ],
                }
            ],
        }
    )


@pytest.fixture
def orders():
    def build(**overrides) -> pa.Table:
        columns = {
            "order_id": pa.array(["O-1", "O-2"]),
            "amount": pa.array([10.0, 20.0]),
            "status": pa.array(["OK", "KO"]),
            "created_at": pa.array(["2025-03-21T18:17:28+00:00", "2025-03-22T10:00:00+00:00"]),
            "note": pa.array(["a", None], pa.string()),
        }
        columns.update(overrides)
        return pa.table({k: v for k, v in columns.items() if v is not None})

    return build

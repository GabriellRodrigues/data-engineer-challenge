import duckdb
import pyarrow as pa

from src.data_contract.record_validator import REFUSED_REASONS_COLUMN, split_records


def test_valid_records_are_accepted(contract, orders):
    table = orders()
    valid, refused = split_records(duckdb.connect(), table, contract)

    assert valid.equals(table)
    assert refused.num_rows == 0


def test_invalid_records_are_refused_with_reasons(contract, orders):
    table = orders(
        order_id=pa.array(["O-1", "X-2", None, "O-4"]),
        amount=pa.array([10.0, -1.0, 5.0, 7.0]),
        status=pa.array(["OK", "KO", "OK", "MAYBE"]),
        created_at=pa.array(["2025-03-21T18:17:28+00:00", "2025-03-21", "2025-03-21", "not-a-date"]),
        note=pa.array([None, None, None, None], pa.string()),
    )
    valid, refused = split_records(duckdb.connect(), table, contract)

    assert valid.column_names == table.column_names
    assert valid["order_id"].to_pylist() == ["O-1"]
    assert refused.num_rows == 3
    assert refused[REFUSED_REASONS_COLUMN].to_pylist() == [
        ["order_id: value does not match pattern O-[0-9]+", "amount: value below minimum 0"],
        ["order_id: required value is null"],
        ["status: value not in ['OK', 'KO']", "created_at: value is not a valid ISO 8601 date/timestamp"],
    ]


def test_length_and_maximum_rules(contract):
    contract = {**contract, "schema": [{"name": "orders", "properties": [
        {"name": "order_id", "logicalType": "string", "physicalType": "varchar", "required": True,
         "logicalTypeOptions": {"minLength": 3, "maxLength": 4}},
        {"name": "amount", "logicalType": "number", "physicalType": "double", "required": True,
         "logicalTypeOptions": {"maximum": 100}},
    ]}]}
    table = pa.table({"order_id": ["O-1", "O", "O-1234"], "amount": [100.0, 1.0, 101.0]})

    valid, refused = split_records(duckdb.connect(), table, contract)

    assert valid["order_id"].to_pylist() == ["O-1"]
    assert refused[REFUSED_REASONS_COLUMN].to_pylist() == [
        ["order_id: shorter than 3"],
        ["order_id: longer than 4", "amount: value above maximum 100"],
    ]


def test_optional_null_values_are_accepted(contract, orders):
    valid, refused = split_records(duckdb.connect(), orders(note=pa.array([None, None], pa.string())), contract)

    assert valid.num_rows == 2
    assert refused.num_rows == 0

from pathlib import Path

from src.data_contract.schema_validator import PHYSICAL_TYPES
from src.utils.yaml_reader import read_yaml

CONTRACT_KIND = "DataContract"
REQUIRED_CONTRACT_FIELDS = ("apiVersion", "kind", "id", "name", "version", "status", "schema")
REQUIRED_OBJECT_FIELDS = ("name", "properties")
REQUIRED_PROPERTY_FIELDS = ("name", "logicalType", "physicalType", "required")
LOGICAL_TYPES = {"string", "date", "number", "integer", "object", "array", "boolean"}


class ContractValidationError(ValueError):
    def __init__(self, contract_name: str, errors: list[str]):
        self.contract_name = contract_name
        self.errors = errors
        details = "\n".join(f"  - {e}" for e in errors)
        super().__init__(f"Invalid data contract '{contract_name}':\n{details}")


def _missing(fields: tuple[str, ...], node: dict) -> list[str]:
    return [f for f in fields if node.get(f) in (None, "", [])]


def _validate_property(prop: dict, location: str) -> list[str]:
    errors = [f"{location}: missing required field '{f}'" for f in _missing(REQUIRED_PROPERTY_FIELDS, prop)]

    logical_type = prop.get("logicalType")
    if logical_type and logical_type not in LOGICAL_TYPES:
        errors.append(f"{location}: invalid logicalType '{logical_type}'")

    physical_type = prop.get("physicalType")
    if physical_type and physical_type not in PHYSICAL_TYPES:
        errors.append(f"{location}: unsupported physicalType '{physical_type}'")

    if "required" in prop and not isinstance(prop["required"], bool):
        errors.append(f"{location}: 'required' must be a boolean")

    return errors


def _validate_object(obj: dict, location: str) -> list[str]:
    errors = [f"{location}: missing required field '{f}'" for f in _missing(REQUIRED_OBJECT_FIELDS, obj)]

    properties = obj.get("properties") or []
    if not isinstance(properties, list):
        return errors + [f"{location}: 'properties' must be a list"]

    seen = set()
    for i, prop in enumerate(properties):
        if not isinstance(prop, dict):
            errors.append(f"{location}.properties[{i}]: must be a mapping")
            continue

        name = prop.get("name")
        prop_location = f"{location}.{name or f'properties[{i}]'}"
        if name in seen:
            errors.append(f"{prop_location}: duplicated property name")
        seen.add(name)

        errors.extend(_validate_property(prop, prop_location))

    return errors


def validate_contract(contract: dict) -> dict:
    contract_name = contract.get("name") or contract.get("id") or "<unknown>"
    errors = [f"missing required field '{f}'" for f in _missing(REQUIRED_CONTRACT_FIELDS, contract)]

    kind = contract.get("kind")
    if kind and kind != CONTRACT_KIND:
        errors.append(f"'kind' must be '{CONTRACT_KIND}', got '{kind}'")

    schema = contract.get("schema") or []
    if not isinstance(schema, list):
        errors.append("'schema' must be a list")
        schema = []

    for i, obj in enumerate(schema):
        if not isinstance(obj, dict):
            errors.append(f"schema[{i}]: must be a mapping")
            continue
        errors.extend(_validate_object(obj, f"schema.{obj.get('name') or i}"))

    if errors:
        raise ContractValidationError(contract_name, errors)

    return contract


def load_contract(name: str, contracts_dir: Path) -> dict:
    return validate_contract(read_yaml(contracts_dir / f"{name}.yml"))

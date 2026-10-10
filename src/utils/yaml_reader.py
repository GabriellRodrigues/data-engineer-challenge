from pathlib import Path

import yaml


def read_yaml(path: str | Path) -> dict:
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"YAML file not found: {path}")

    with path.open(encoding="utf-8") as f:
        content = yaml.safe_load(f)

    if not isinstance(content, dict):
        raise ValueError(f"YAML file must contain a mapping at the top level: {path}")

    return content

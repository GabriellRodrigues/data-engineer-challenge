import pytest

from src.utils.yaml_reader import read_yaml


def test_reads_mapping(tmp_path):
    path = tmp_path / "config.yml"
    path.write_text("name: orders\nversion: 1.0.0\nitems: [a, b]\n", encoding="utf-8")

    assert read_yaml(path) == {"name": "orders", "version": "1.0.0", "items": ["a", "b"]}


def test_accepts_string_path(tmp_path):
    path = tmp_path / "config.yml"
    path.write_text("a: 1\n", encoding="utf-8")

    assert read_yaml(str(path)) == {"a": 1}


def test_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="YAML file not found"):
        read_yaml(tmp_path / "missing.yml")


@pytest.mark.parametrize("content", ["- a\n- b\n", "just text\n", ""])
def test_non_mapping_content_raises(tmp_path, content):
    path = tmp_path / "config.yml"
    path.write_text(content, encoding="utf-8")

    with pytest.raises(ValueError, match="must contain a mapping"):
        read_yaml(path)

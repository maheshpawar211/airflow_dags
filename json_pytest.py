import pytest
from your_module import parse_json  # Replace 'your_module' with your actual module name

def test_valid_key():
    json_str = '{"name": "Alice", "age": 30, "city": "New York"}'
    assert parse_json(json_str, "name") == "Alice"
    assert parse_json(json_str, "age") == 30

def test_missing_key():
    json_str = '{"name": "Alice", "age": 30, "city": "New York"}'
    assert parse_json(json_str, "country") is None  # Missing key should return None

def test_invalid_json():
    json_str = '{"name": "Alice", "age": 30, "city": "New York"'  # Missing closing brace
    with pytest.raises(ValueError, match="Invalid JSON format"):
        parse_json(json_str, "name")

def test_empty_json():
    json_str = '{}'
    assert parse_json(json_str, "name") is None  # Empty JSON should return None

def test_non_string_key():
    json_str = '{"1": "one", "2": "two"}'
    assert parse_json(json_str, 1) is None  # Non-string keys should return None

`
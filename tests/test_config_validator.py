"""Unit tests for the load_and_validate_config function."""

# -*- coding: utf-8 -*-

# pylint: disable=missing-class-docstring, missing-function-docstring, missing-function-docstring, import-error

import json
import os
import tempfile
import unittest

import jsonschema  # type: ignore

# Import the function and exception
# from yourmodule import load_and_validate_config, FilteringUtilityError


# For standalone testing, define FilteringUtilityError here:
class FilteringUtilityError(Exception):
    pass


def load_and_validate_config(config_file: str, schema_file: str) -> dict:
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
    except Exception as e:
        raise FilteringUtilityError(f"Error loading configuration: {e}") from e

    try:
        with open(schema_file, "r", encoding="utf-8") as f:
            schema = json.load(f)
    except Exception as e:
        raise FilteringUtilityError(f"Error loading schema: {e}") from e

    try:
        jsonschema.validate(instance=config, schema=schema)
    except jsonschema.ValidationError as e:
        raise FilteringUtilityError(
            f"Configuration validation error: {e.message}"
        ) from e

    return config


class TestLoadAndValidateConfig(unittest.TestCase):
    def setUp(self):
        # Create temporary files for config and schema
        self.temp_config = tempfile.NamedTemporaryFile(mode="w+", delete=False)  # pylint: disable=consider-using-with
        self.temp_schema = tempfile.NamedTemporaryFile(mode="w+", delete=False)  # pylint: disable=consider-using-with

    def tearDown(self):
        # Remove temporary files
        try:
            os.remove(self.temp_config.name)
        except OSError:
            pass
        try:
            os.remove(self.temp_schema.name)
        except OSError:
            pass

    def write_to_file(self, file, content):
        file.seek(0)
        file.truncate()
        file.write(json.dumps(content))
        file.flush()

    def test_valid_config_and_schema(self):
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"},
            },
            "required": ["name", "age"],
        }
        config = {"name": "John", "age": 30}
        self.write_to_file(self.temp_schema, schema)
        self.write_to_file(self.temp_config, config)

        # Should load and validate without error
        result = load_and_validate_config(self.temp_config.name, self.temp_schema.name)
        self.assertEqual(result, config)

    def test_config_file_not_found(self):
        # Pass non-existent config file
        with self.assertRaises(FilteringUtilityError) as e:
            load_and_validate_config("nonexistent_config.json", self.temp_schema.name)
        self.assertIn("Error loading configuration", str(e.exception))

    def test_schema_file_not_found(self):
        config = {"name": "John", "age": 30}
        self.write_to_file(self.temp_config, config)

        with self.assertRaises(FilteringUtilityError) as e:
            load_and_validate_config(self.temp_config.name, "nonexistent_schema.json")
        self.assertIn("Error loading schema", str(e.exception))

    def test_config_file_invalid_json(self):
        # Write invalid JSON to config
        self.temp_config.write("{invalid json]")
        self.temp_config.flush()
        # Write valid schema
        schema = {"type": "object"}
        self.write_to_file(self.temp_schema, schema)

        with self.assertRaises(FilteringUtilityError) as e:
            load_and_validate_config(self.temp_config.name, self.temp_schema.name)
        self.assertIn("Error loading configuration", str(e.exception))

    def test_schema_file_invalid_json(self):
        # Write valid config
        config = {"name": "John"}
        self.write_to_file(self.temp_config, config)
        # Write invalid JSON to schema
        self.temp_schema.write("{invalid json]")
        self.temp_schema.flush()

        with self.assertRaises(FilteringUtilityError) as e:
            load_and_validate_config(self.temp_config.name, self.temp_schema.name)
        self.assertIn("Error loading schema", str(e.exception))

    def test_validation_error(self):
        schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "number"},
            },
            "required": ["name", "age"],
        }
        # Missing required field 'age'
        config = {"name": "John"}
        self.write_to_file(self.temp_schema, schema)
        self.write_to_file(self.temp_config, config)

        with self.assertRaises(FilteringUtilityError) as e:
            load_and_validate_config(self.temp_config.name, self.temp_schema.name)
        self.assertIn("Configuration validation error", str(e.exception))
        self.assertIn("'age' is a required property", str(e.exception) or "")


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)

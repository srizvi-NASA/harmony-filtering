"""
Module for validating JSON configuration files against a JSON Schema.

This module provides a function to load a JSON configuration file and validate
it against a given JSON schema. If the configuration file fails to load or does
not conform to the schema, a FilteringUtilityError is raised.
"""

import json

import jsonschema

from harmony_filtering_service.exceptions import FilteringUtilityError


def load_and_validate_config(config_file: str, schema_file: str) -> dict:
    """
    Loads a JSON configuration file and validates it against a JSON Schema.

    Parameters:
        config_file (str): Path to the configuration JSON file (e.g., config.json).
        schema_file (str): Path to the JSON schema file (e.g., config_schema.json).

    Returns:
        dict: The validated configuration.

    Raises:
        FilteringUtilityError: If the configuration fails to load or does not validate.
    """
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

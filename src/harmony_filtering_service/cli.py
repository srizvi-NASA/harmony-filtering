"""
Command-line interface for Filtering Utility.

This module provides a CLI entry point that loads settings and configuration
files, then runs the core processing.
"""

import argparse
import os

from harmony_filtering_service import core
from harmony_filtering_service.config_validator import load_and_validate_config
from harmony_filtering_service.exceptions import FilteringUtilityError


def main() -> None:
    """
    Parse command-line arguments, validate config.json against the schema, and run the filtering process.
    """
    parser = argparse.ArgumentParser(description="Filtering Utility CLI")
    parser.add_argument(
        "--settings",
        default="config/settings.json",
        help="Path to settings.json file",
    )
    parser.add_argument(
        "--config",
        default="config/config.json",
        help="Path to config.json file",
    )
    parser.add_argument(
        "--settings_schema",
        default="config/settings_schema.json",
        help="Path to settings_schema.json file",
    )
    parser.add_argument(
        "--config_schema",
        default="config/config_schema.json",
        help="Path to config_schema.json file",
    )

    args = parser.parse_args()

    if not os.path.exists(args.settings):
        raise FileNotFoundError(f"Settings file '{args.settings}' not found.")
    if not os.path.exists(args.config):
        raise FileNotFoundError(f"Config file '{args.config}' not found.")
    if not os.path.exists(args.settings_schema):
        raise FileNotFoundError(f"Schema file '{args.settings_schema}' not found.")
    if not os.path.exists(args.config_schema):
        raise FileNotFoundError(f"Schema file '{args.config_schema}' not found.")

    try:
        # Validate settings.json against its schema
        settings = load_and_validate_config(args.settings, args.settings_schema)
    except FilteringUtilityError as e:
        print(f"Config validation error: {e}")
        return

    try:
        # Validate config.json against its schema
        config = load_and_validate_config(args.config, args.config_schema)
    except FilteringUtilityError as e:
        print(f"Config validation error: {e}")
        return

    # Now run the core processing using the validated settings and config.
    core.process_products(settings, config)


if __name__ == "__main__":
    main()

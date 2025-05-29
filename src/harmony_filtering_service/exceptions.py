"""
Exceptions module for Filtering Utility.

This module defines custom exceptions used throughout the project.
"""


class FilteringUtilityError(Exception):
    """Base exception for errors in the Filtering Utility."""

    pass


class ParsingError(FilteringUtilityError):
    """Exception raised for errors in parsing input files or filenames."""

    pass


class FileProcessingError(FilteringUtilityError):
    """Exception raised for errors during file processing."""

    pass

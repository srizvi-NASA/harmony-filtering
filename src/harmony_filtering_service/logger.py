"""
Logger module for Filtering Utility.

This module provides a simple logging utility that prints messages
to the console and writes them to a log file if the log level is set to DEBUG.
"""

import datetime
import os
from typing import Optional, Union


class Logger:
    """
    Logger that logs messages to console and to a file.
    """

    def __init__(
        self,
        log_level: str,
        log_to_console: bool,
        log_to_file: bool,
        log_file_path: str,
        granule_name: str,
    ) -> None:
        """
        Initialize the Logger.

        Parameters:
            log_level: The log level (only "DEBUG" will log messages).
            log_to_console: If True, print messages to console.
            log_to_file: If True, write messages to a log file.
            log_file_path: Directory path for log files.
            granule_name: Granule name used in generating the log filename.
        """
        self.log_level = log_level
        self.log_to_console = log_to_console
        self.log_to_file = log_to_file
        self.log_file_path = log_file_path
        self.granule_name = granule_name
        self.log_filename: Optional[str] = None

        if self.log_to_file:
            if not os.path.exists(self.log_file_path):
                os.makedirs(self.log_file_path)
            timestamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
            # Log filename format: {timestamp}_{granule_name}.log
            self.log_filename = os.path.join(
                self.log_file_path, f"{timestamp}_{granule_name}.log"
            )

    def log(self, message: str) -> None:
        """
        Log the given message if the log level is DEBUG.

        Parameters:
            message: The message to log.
        """
        if self.log_level != "DEBUG":
            return
        if self.log_to_console:
            print(message)
        if self.log_to_file and self.log_filename:
            # Use a context manager to open the file for appending
            with open(self.log_filename, "a", encoding="utf-8") as f:
                f.write(message + "\n")

    def close(self) -> None:
        """
        Close the logger.

        This implementation does not keep the file open persistently,
        so there is no resource to close.
        """
        pass


class DummyLogger:
    """
    Dummy logger that does nothing.
    """

    def log(self, message: str) -> None:
        """
        Dummy log method that does nothing.

        Parameters:
            message: The message to log.
        """
        pass

    def close(self) -> None:
        """
        Dummy close method that does nothing.
        """
        pass


def get_logger(
    granule_name: str,
    log_level: str,
    log_to_console: bool,
    log_to_file: bool,
    log_file_path: str,
) -> Union[Logger, DummyLogger]:
    """
    Get a Logger instance if log_level is DEBUG, otherwise return a DummyLogger.

    Parameters:
        granule_name: Granule name for the log file.
        log_level: The log level.
        log_to_console: Whether to log to console.
        log_to_file: Whether to log to file.
        log_file_path: Directory where log files should be stored.

    Returns:
        A Logger instance if log_level is "DEBUG", otherwise a DummyLogger.
    """
    if log_level == "DEBUG":
        return Logger(
            log_level, log_to_console, log_to_file, log_file_path, granule_name
        )
    return DummyLogger()


def log_msg(message: str, logger: Optional[Union[Logger, DummyLogger]]) -> None:
    """
    Log a message using the provided logger.

    Parameters:
        message: The message to log.
        logger: Logger or DummyLogger instance.
    """
    if logger:
        logger.log(message)

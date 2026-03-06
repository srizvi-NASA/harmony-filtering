"""Unit tests for the Logger and DummyLogger classes."""

import os
import shutil
import sys
import tempfile
import time
import unittest
from io import StringIO

from harmony_filtering_service.logger import DummyLogger, Logger, get_logger, log_msg

# pylint: disable=missing-class-docstring, missing-function-docstring, missing-function-docstring
# pylint: disable=broad-exception-caught, redefined-outer-name, too-few-public-methods

# Import the classes and functions from the module under test.
# Assuming the module name is 'logger_module', replace as needed.
# from logger_module import Logger, DummyLogger, get_logger, log_msg

# For standalone testing, include the Logger classes/functions here or import them
# Here, we assume they are imported as per prompt.


class TestLogger(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for log files
        self.temp_dir = tempfile.mkdtemp()
        self.granule_name = "testgranule"

    def tearDown(self):
        # Remove temporary directory and all files in it
        shutil.rmtree(self.temp_dir)

    def test_logger_init_creates_logfile_correctly(self):
        logger = Logger(
            log_level="DEBUG",
            log_to_console=False,
            log_to_file=True,
            log_file_path=self.temp_dir,
            granule_name=self.granule_name,
        )
        self.assertIsNotNone(logger.log_filename)
        self.assertTrue(logger.log_filename.startswith(self.temp_dir))
        self.assertTrue(logger.log_filename.endswith(f"{self.granule_name}.log"))
        # The directory should exist
        self.assertTrue(os.path.exists(self.temp_dir))

    def test_logger_init_creates_log_dir_if_missing(self):
        # Remove temp_dir first to test dir creation
        shutil.rmtree(self.temp_dir)

        # Directory does not exist now
        self.assertFalse(os.path.exists(self.temp_dir))

        logger = Logger(
            log_level="DEBUG",
            log_to_console=False,
            log_to_file=True,
            log_file_path=self.temp_dir,
            granule_name=self.granule_name,
        )
        self.assertTrue(os.path.exists(self.temp_dir))
        self.assertIsNotNone(logger.log_filename)

    def test_logger_log_to_console_only(self):
        logger = Logger(
            log_level="DEBUG",
            log_to_console=True,
            log_to_file=False,
            log_file_path=self.temp_dir,
            granule_name=self.granule_name,
        )
        message = "Console only log message"
        captured_output = StringIO()
        sys.stdout = captured_output
        logger.log(message)
        sys.stdout = sys.__stdout__
        self.assertIn(message, captured_output.getvalue())

    def test_logger_log_to_file_only(self):
        logger = Logger(
            log_level="DEBUG",
            log_to_console=False,
            log_to_file=True,
            log_file_path=self.temp_dir,
            granule_name=self.granule_name,
        )
        message = "File only log message"
        logger.log(message)

        # Wait briefly to ensure file has been written
        time.sleep(0.1)

        with open(logger.log_filename, "r", encoding="utf-8") as f:
            contents = f.read()
        self.assertIn(message, contents)

    def test_logger_log_to_console_and_file(self):
        logger = Logger(
            log_level="DEBUG",
            log_to_console=True,
            log_to_file=True,
            log_file_path=self.temp_dir,
            granule_name=self.granule_name,
        )
        message = "Console and file log message"
        # Capture console output
        captured_output = StringIO()
        sys.stdout = captured_output
        logger.log(message)
        sys.stdout = sys.__stdout__

        # Check console output
        self.assertIn(message, captured_output.getvalue())

        # Wait briefly to ensure file has been written
        time.sleep(0.1)

        # Check file output
        with open(logger.log_filename, "r", encoding="utf-8") as f:
            contents = f.read()
        self.assertIn(message, contents)

    def test_logger_log_level_not_debug(self):
        logger = Logger(
            log_level="INFO",
            log_to_console=True,
            log_to_file=True,
            log_file_path=self.temp_dir,
            granule_name=self.granule_name,
        )
        message = "Message should not be logged"

        captured_output = StringIO()
        sys.stdout = captured_output
        logger.log(message)
        sys.stdout = sys.__stdout__

        # Nothing should be printed or written
        self.assertEqual(captured_output.getvalue(), "")
        if logger.log_filename and os.path.exists(logger.log_filename):
            with open(logger.log_filename, "r", encoding="utf-8") as f:
                contents = f.read()
            self.assertNotIn(message, contents)

    def test_logger_close_method_does_not_raise(self):
        logger = Logger(
            log_level="DEBUG",
            log_to_console=True,
            log_to_file=True,
            log_file_path=self.temp_dir,
            granule_name=self.granule_name,
        )
        try:
            logger.close()
        except Exception as e:
            self.fail(f"Logger.close() raised an exception: {e}")

    def test_dummy_logger_log_and_close_do_nothing(self):
        dummy = DummyLogger()
        try:
            dummy.log("Any message")
            dummy.close()
        except Exception as e:
            self.fail(f"DummyLogger methods raised an exception: {e}")

    def test_get_logger_returns_logger_for_debug(self):
        logger = get_logger(
            granule_name=self.granule_name,
            log_level="DEBUG",
            log_to_console=False,
            log_to_file=False,
            log_file_path=self.temp_dir,
        )
        self.assertIsInstance(logger, Logger)

    def test_get_logger_returns_dummylogger_for_non_debug(self):
        dummy_logger = get_logger(
            granule_name=self.granule_name,
            log_level="INFO",
            log_to_console=False,
            log_to_file=False,
            log_file_path=self.temp_dir,
        )
        self.assertIsInstance(dummy_logger, DummyLogger)

    def test_log_msg_calls_logger_log(self):
        messages = []

        class MockLogger:
            def log(self, msg):
                messages.append(msg)

        logger = MockLogger()
        test_message = "Test log_msg message"
        log_msg(test_message, logger)
        self.assertIn(test_message, messages)

    def test_log_msg_none_logger_does_nothing(self):
        # Should not raise or print anything
        try:
            log_msg("message", None)
        except Exception as e:
            self.fail(f"log_msg raised an exception with None logger: {e}")


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)

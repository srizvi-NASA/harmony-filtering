"""
Test cases for the compare_nc_files function."""

# pylint: disable=missing-class-docstring, missing-function-docstring, missing-function-docstring, unused-argument, unused-import, import-error, too-few-public-methods

import unittest
from unittest.mock import patch

import numpy as np  # type: ignore

from harmony_filtering_service.compare import compare_nc_files

# Assuming compare_nc_files, compare_nc_groups, log_msg, and ncDataset are imported from the module to test
# Here, we will mock ncDataset and log_msg for testing purposes.


# ######################################################################
# Mocking the log_msg function to capture log messages
# ######################################################################
def log_msg(msg, logger=None):
    """Mock log_msg function to capture log messages for testing"""
    if logger:
        logger(msg)
    else:
        print(msg)


class DummyDim:
    """Dummy dimension class for testing"""

    def __init__(self, length):
        self.length = length

    def __len__(self):
        return self.length


class DummyVar:
    """Dummy variable class for testing"""

    def __init__(self, dims=(), attr_dict=None):
        self.dimensions = dims
        self._attrs = attr_dict or {}

    def ncattrs(self):
        """Return a list of attribute names"""
        return list(self._attrs.keys())

    def getncattr(self, attr):
        """Get the value of an attribute"""
        return self._attrs[attr]


# ######################################################################
# Dummy NetCDF class to simulate the behavior of netCDF4.Dataset
# This is a simplified version and does not cover all netCDF4 features.
# It is used to create mock objects for testing.
# ######################################################################
class DummyNetCDF:
    """Dummy NetCDF class for testing"""

    def __init__(self, attrs=None, dims=None, variables=None):
        self._attrs = attrs or {}
        self.dimensions = dims or {}  # dict of name -> DummyDim
        self.variables = variables or {}  # dict of name -> DummyVar

        # Pre-generate list of attr names for ncattrs()
        self._attr_list = list(self._attrs.keys())

    def ncattrs(self):
        """Return a list of attribute names"""
        return self._attr_list

    def getncattr(self, attr):
        """Get the value of an attribute"""
        return self._attrs[attr]

    def close(self):
        """mock close method"""
        pass


# ######################################################################
# Test cases for compare_nc_files function
# ######################################################################
class TestCompareNCFiles(unittest.TestCase):
    def setUp(self):
        self.logged = []

        def logger(msg):
            self.logged.append(msg)

        self.logger = logger

    @patch("harmony_filtering_service.compare.ncDataset")
    @patch("harmony_filtering_service.compare.compare_nc_groups")
    @patch("harmony_filtering_service.compare.log_msg", side_effect=log_msg)
    def test_compare_nc_files_basic(
        self, mock_log_msg, mock_compare_nc_groups, mock_nc_dataset
    ):
        """Test the basic functionality of compare_nc_files"""

        # Setup dummy original nc and filtered nc
        original_attrs = {"title": "Dataset", "numbers": np.array([1, 2, 3])}
        filtered_attrs = {"title": "Dataset", "numbers": np.array([1, 2, 3])}

        orig_dims = {"time": DummyDim(5), "lat": DummyDim(10)}
        filt_dims = {"time": DummyDim(5), "lat": DummyDim(10)}

        # Create dummy netCDF objects
        dummy_orig = DummyNetCDF(attrs=original_attrs, dims=orig_dims)
        dummy_filt = DummyNetCDF(attrs=filtered_attrs, dims=filt_dims)

        # Setup the mock for ncDataset to return these dummy objects in order
        mock_nc_dataset.side_effect = [dummy_orig, dummy_filt]

        # Run the function under test
        compare_nc_files("orig.nc", "filt.nc", logger=self.logger)

        # Assertions

        # It should log the initial compare message
        self.assertTrue(any("Comparing original file" in msg for msg in self.logged))

        # It should call compare_nc_groups once
        mock_compare_nc_groups.assert_called_once_with(
            dummy_orig, dummy_filt, current_group="", logger=self.logger
        )

        # It should log Comparison complete
        self.assertTrue(any("Comparison complete" in msg for msg in self.logged))

        # No messages about missing attributes or dimension mismatch expected
        miss_attr_msgs = [m for m in self.logged if "missing" in m.lower()]
        self.assertEqual(len(miss_attr_msgs), 0)

        size_mismatch_msgs = [m for m in self.logged if "size mismatch" in m.lower()]
        self.assertEqual(len(size_mismatch_msgs), 0)

    @patch("harmony_filtering_service.compare.ncDataset")
    @patch("harmony_filtering_service.compare.compare_nc_groups")
    @patch("harmony_filtering_service.compare.log_msg", side_effect=log_msg)
    def test_compare_nc_files_with_missing_and_mismatched_attrs_dims(
        self, mock_log_msg, mock_compare_nc_groups, mock_nc_dataset
    ):
        """Test handling of missing and mismatched attributes and dimensions"""

        # Original has attributes A and B; filtered missing B and mismatched A
        original_attrs = {
            "A": 42,
            "B": "foo",
        }
        filtered_attrs = {
            "A": 43,  # mismatch
            # missing B
        }

        orig_dims = {"x": DummyDim(5), "y": DummyDim(10)}
        filt_dims = {
            "x": DummyDim(6),  # size mismatch
            # missing y
        }

        dummy_orig = DummyNetCDF(attrs=original_attrs, dims=orig_dims)
        dummy_filt = DummyNetCDF(attrs=filtered_attrs, dims=filt_dims)

        mock_nc_dataset.side_effect = [dummy_orig, dummy_filt]

        compare_nc_files("o.nc", "f.nc", logger=self.logger)

        # Check logs for missing attribute 'B'
        self.assertIn("Global attribute 'B' missing in filtered file.", self.logged)

        # Check logs for attribute 'A' mismatch
        self.assertTrue(
            any("Global attribute 'A' mismatch" in msg for msg in self.logged),
            "Did not find mismatch message for attribute 'A'",
        )

        # Check logs for missing dimension 'y'
        self.assertIn("Root dimension 'y' missing in filtered file.", self.logged)

        # Check logs for size mismatch of dimension 'x'
        self.assertTrue(
            any("Root dimension 'x' size mismatch" in msg for msg in self.logged)
        )

    @patch("harmony_filtering_service.compare.ncDataset")
    @patch("harmony_filtering_service.compare.compare_nc_groups")
    @patch("harmony_filtering_service.compare.log_msg", side_effect=log_msg)
    def test_compare_nc_files_array_attribute_comparison(
        self, mock_log_msg, mock_compare_nc_groups, mock_nc_dataset
    ):
        """Test array attribute comparison"""

        # Attributes are arrays and differ
        original_attrs = {"arr": np.array([1, 2, 3])}
        filtered_attrs = {"arr": np.array([1, 2, 4])}  # differs in last element
        orig_dims = {}
        filt_dims = {}

        dummy_orig = DummyNetCDF(attrs=original_attrs, dims=orig_dims)
        dummy_filt = DummyNetCDF(attrs=filtered_attrs, dims=filt_dims)

        mock_nc_dataset.side_effect = [dummy_orig, dummy_filt]

        compare_nc_files("o.nc", "f.nc", logger=self.logger)

        # Check that mismatch is logged for array attribute
        self.assertTrue(
            any("Global attribute 'arr' mismatch." in msg for msg in self.logged)
        )


# ######################################################################
# Check that the correct number of elements is mentioned
# ######################################################################
if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
